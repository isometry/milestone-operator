/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package membership

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

const listPageSize = 500

// Evaluator computes, from a live cluster, the verdict the operator would
// record for an owner, using the same normalisation and reduction rules.
type Evaluator struct {
	// Resolver should be uncached (or freshly built) so a just-installed or
	// just-removed CRD is seen as the operator would see it.
	Resolver discovery.Resolver
	// Mapper translates the resolved GVK into a resource. When it also
	// implements apimeta.ResettableRESTMapper it is reset once on NoKindMatch.
	Mapper     apimeta.RESTMapper
	Dynamic    dynamic.Interface
	Namespaces NamespaceLister
	// Now defaults to time.Now.
	Now func() time.Time
}

// specEntry is the owner-kind-independent view of a spec.dependsOn[] entry.
type specEntry struct {
	name   string
	target TargetRef
	// listNamespaces overrides the namespaces listed for a namespaced kind;
	// nil means one cluster-wide list.
	listNamespaces []string
}

// Milestone evaluates m. The error is non-nil only when nothing could be
// evaluated at all; per-dependency failures are reported in the Report.
func (e *Evaluator) Milestone(ctx context.Context, m *apiv1.Milestone) (Report, error) {
	deps, errs := NormalizeMilestone(ctx, e.Resolver, m)
	entries := make([]specEntry, 0, len(m.Spec.DependsOn))
	for _, d := range m.Spec.DependsOn {
		entries = append(entries, specEntry{
			name:           d.Name,
			target:         targetRef(d.Target, nil, nil),
			listNamespaces: []string{m.Namespace},
		})
	}
	owner := OwnerRef{Kind: "Milestone", Namespace: m.Namespace, Name: m.Name, Generation: m.Generation}
	return e.evaluate(ctx, owner, &m.Status.MilestoneStatusBase, entries, deps, errs)
}

// ClusterMilestone evaluates cm. The error is non-nil only when nothing could
// be evaluated at all; per-dependency failures are reported in the Report.
func (e *Evaluator) ClusterMilestone(ctx context.Context, cm *apiv1.ClusterMilestone) (Report, error) {
	deps, errs := NormalizeClusterMilestone(ctx, e.Resolver, e.Namespaces, cm)
	entries := make([]specEntry, 0, len(cm.Spec.DependsOn))
	for _, d := range cm.Spec.DependsOn {
		entries = append(entries, specEntry{
			name:           d.Name,
			target:         targetRef(d.Target.TargetSpec, d.Target.Namespaces, d.Target.NamespaceSelector),
			listNamespaces: d.Target.Namespaces,
		})
	}
	owner := OwnerRef{Kind: "ClusterMilestone", Name: cm.Name, Generation: cm.Generation}
	return e.evaluate(ctx, owner, &cm.Status.MilestoneStatusBase, entries, deps, errs)
}

func (e *Evaluator) evaluate(ctx context.Context, owner OwnerRef, recorded *apiv1.MilestoneStatusBase, entries []specEntry, deps []Dependency, errs []Error) (Report, error) {
	for _, err := range errs {
		if err.Name == "" {
			return Report{}, err
		}
	}
	byName := make(map[string]Dependency, len(deps))
	for _, d := range deps {
		byName[d.Name] = d
	}
	errByName := make(map[string]Error, len(errs))
	for _, err := range errs {
		if _, ok := errByName[err.Name]; !ok {
			errByName[err.Name] = err
		}
	}

	stale := recorded.ObservedGeneration < owner.Generation
	perDep := make(map[string]apiv1.DependencyStatus, len(deps))
	reports := make([]DependencyReport, 0, len(entries))
	for _, ent := range entries {
		dr := DependencyReport{Name: ent.name, Target: ent.target}
		if d, ok := byName[ent.name]; ok {
			perDep[ent.name] = e.evaluateDependency(ctx, d, ent.listNamespaces, &dr)
		} else if err, ok := errByName[ent.name]; ok {
			dr.Error = err.Error()
		}
		reports = append(reports, dr)
	}

	rollups := Rollups(perDep, errs)
	for i := range reports {
		dr := &reports[i]
		dr.Status = rollups[dr.Name]
		dr.Recorded = recordedDependency(recorded.DependsOn, dr.Name)
		dr.Drift = !stale && dr.Recorded != nil &&
			(dr.Recorded.Ready != dr.Status.Ready || dr.Recorded.Reason != dr.Status.Reason)
	}

	ready, reason, message := status.ReduceOwner(rollups)
	return Report{
		Owner: owner,
		Recorded: RecordedStatus{
			Conditions:         recorded.Conditions,
			ObservedGeneration: recorded.ObservedGeneration,
			LastEvaluatedTime:  recorded.LastEvaluatedTime,
			Stale:              stale,
		},
		Ready:        ready,
		Reason:       reason,
		Message:      message,
		Summary:      status.SummarizeOwner(rollups),
		Dependencies: reports,
		EvaluatedAt:  metav1.NewTime(e.now()),
	}, nil
}

// evaluateDependency returns d's rollup and fills dr's members and failure
// details. Failures mirror the operator: an unmappable kind is
// WatchSetupFailed (its informer could not start), a failed list is
// ListFailed, and both force Ready=Unknown.
func (e *Evaluator) evaluateDependency(ctx context.Context, d Dependency, listNamespaces []string, dr *DependencyReport) apiv1.DependencyStatus {
	mapping, err := e.restMapping(d.GVK)
	if err != nil {
		dr.Error = err.Error()
		return FailedRollup(d.Name, d.GVK, apiv1.ReasonWatchSetupFailed)
	}
	if d.Scope != apimeta.RESTScopeNameNamespace || len(listNamespaces) == 0 {
		listNamespaces = []string{metav1.NamespaceAll}
	}

	var resources []status.Resource
	for _, ns := range listNamespaces {
		objs, err := e.list(ctx, mapping.Resource, ns, d)
		if err != nil {
			dr.Error = err.Error()
			dr.Forbidden = apierrors.IsForbidden(err)
			return FailedRollup(d.Name, d.GVK, apiv1.ReasonListFailed)
		}
		for i := range objs {
			u := &objs[i]
			// A server may ignore the label selector; membership is decided
			// here, exactly as the operator filters its informer cache.
			if !d.Admits(u.GetNamespace(), u.GetLabels()) {
				continue
			}
			if u.GetKind() == "" {
				u.SetGroupVersionKind(d.GVK)
			}
			resources = append(resources, status.Compute(u))
		}
	}

	sort.Slice(resources, func(i, j int) bool {
		a, b := resources[i], resources[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	dr.Members = make([]Member, 0, len(resources))
	for _, r := range resources {
		dr.Members = append(dr.Members, Member{Resource: r, Blocking: r.Blocks(d.SuspendPolicy)})
	}
	return status.ReduceDependency(d.Name, d.GVK.Group, d.GVK.Version, d.GVK.Kind, resources, d.EmptySetPolicy, d.SuspendPolicy)
}

// restMapping asks for the resolver's version, which is the one the operator's
// informer watches, rather than whatever version the mapper prefers.
func (e *Evaluator) restMapping(gvk schema.GroupVersionKind) (*apimeta.RESTMapping, error) {
	if e.Mapper == nil {
		return nil, errors.New("no REST mapper configured")
	}
	mapping, err := e.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil && apimeta.IsNoMatchError(err) {
		// Discovery just resolved this kind, so a miss means the mapper's
		// cache predates the CRD.
		if rm, ok := e.Mapper.(apimeta.ResettableRESTMapper); ok {
			rm.Reset()
			mapping, err = e.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("map %s: %w", gvk, err)
	}
	return mapping, nil
}

func (e *Evaluator) list(ctx context.Context, gvr schema.GroupVersionResource, namespace string, d Dependency) ([]unstructured.Unstructured, error) {
	var ri dynamic.ResourceInterface = e.Dynamic.Resource(gvr)
	if namespace != metav1.NamespaceAll {
		ri = e.Dynamic.Resource(gvr).Namespace(namespace)
	}
	opts := metav1.ListOptions{Limit: listPageSize}
	if d.Selector != nil && !d.Selector.Empty() {
		opts.LabelSelector = d.Selector.String()
	}
	var out []unstructured.Unstructured
	for {
		l, err := ri.List(ctx, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, l.Items...)
		if l.GetContinue() == "" {
			return out, nil
		}
		opts.Continue = l.GetContinue()
	}
}

func (e *Evaluator) now() time.Time {
	if e.Now == nil {
		return time.Now()
	}
	return e.Now()
}

func recordedDependency(recorded []apiv1.DependencyStatus, name string) *apiv1.DependencyStatus {
	for i := range recorded {
		if recorded[i].Name == name {
			d := recorded[i]
			return &d
		}
	}
	return nil
}

func targetRef(t apiv1.TargetSpec, namespaces []string, nsSelector *metav1.LabelSelector) TargetRef {
	return TargetRef{
		Group:             t.Group,
		Version:           t.Version,
		Kind:              t.Kind,
		Selector:          formatSelector(t.Selector),
		Namespaces:        namespaces,
		NamespaceSelector: formatSelector(nsSelector),
	}
}

// formatSelector renders nil as "" rather than FormatLabelSelector's "<none>",
// so JSON output omits an absent selector.
func formatSelector(ls *metav1.LabelSelector) string {
	if ls == nil {
		return ""
	}
	return metav1.FormatLabelSelector(ls)
}
