/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	k8sdiscovery "k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/restmapper"
	clienttesting "k8s.io/client-go/testing"
)

const (
	nsFlux     = "flux-system"
	nsApps     = "apps"
	groupKust  = "kustomize.toolkit.fluxcd.io"
	kindKust   = "Kustomization"
	resKust    = "kustomizations"
	labelWave  = "wave"
	wave0      = "0"
	wave1      = "1"
	condReady  = "Ready"
	keyType    = "type"
	keyStatus  = "status"
	keyReason  = "reason"
	keyMessage = "message"
	verbList   = "list"
	verbGet    = "get"
	verbWatch  = "watch"
	nameWave0  = "wave-0"
	nameWave1  = "wave-1"
	namePlat   = "platform"
)

var (
	fixedNow  = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	gvrKust   = schema.GroupVersionResource{Group: groupKust, Version: "v1", Resource: resKust}
	gvrCM     = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	gvrDeploy = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
)

func fakeDiscovery() *fakediscovery.FakeDiscovery {
	res := func(name, kind string, namespaced bool, short ...string) metav1.APIResource {
		return metav1.APIResource{
			Name: name, Kind: kind, Namespaced: namespaced, ShortNames: short,
			Verbs: metav1.Verbs{verbList, verbGet, verbWatch},
		}
	}
	return &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{
			res("configmaps", "ConfigMap", true, "cm"),
			res("namespaces", "Namespace", false, "ns"),
		}},
		{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{
			res("deployments", "Deployment", true, "deploy"),
		}},
		{GroupVersion: groupKust + "/v1", APIResources: []metav1.APIResource{
			res(resKust, kindKust, true, "ks"),
		}},
		{GroupVersion: apiv1.GroupVersion.String(), APIResources: []metav1.APIResource{
			res("milestones", "Milestone", true, argMile),
			res("clustermilestones", "ClusterMilestone", false, argCmile),
		}},
	}}}
}

type fakeClients struct {
	dyn    *dynamicfake.FakeDynamicClient
	disc   *fakediscovery.FakeDiscovery
	mapper apimeta.RESTMapper
	kube   *kubefake.Clientset
}

func (f *fakeClients) Dynamic() (dynamic.Interface, error)                 { return f.dyn, nil }
func (f *fakeClients) Discovery() (k8sdiscovery.DiscoveryInterface, error) { return f.disc, nil }
func (f *fakeClients) RESTMapper() (apimeta.RESTMapper, error)             { return f.mapper, nil }
func (f *fakeClients) Kube() (kubernetes.Interface, error)                 { return f.kube, nil }
func (f *fakeClients) DefaultNamespace() (string, error)                   { return nsFlux, nil }

type harness struct {
	clients *fakeClients
	out     *bytes.Buffer
	errOut  *bytes.Buffer
}

// newHarness serves dynObjs through the dynamic client and kubeObjs through
// the typed clientset. The mapper expands short names the way kubectl's does.
func newHarness(t *testing.T, dynObjs []runtime.Object, kubeObjs ...runtime.Object) *harness {
	t.Helper()
	disc := fakeDiscovery()
	cached := memory.NewMemCacheClient(disc)
	mapper := restmapper.NewShortcutExpander(restmapper.NewDeferredDiscoveryRESTMapper(cached), cached, nil)
	listKinds := map[schema.GroupVersionResource]string{
		apiv1.GroupVersion.WithResource("milestones"):        "MilestoneList",
		apiv1.GroupVersion.WithResource("clustermilestones"): "ClusterMilestoneList",
		gvrKust:                                 "KustomizationList",
		gvrCM:                                   "ConfigMapList",
		gvrDeploy:                               "DeploymentList",
		{Version: "v1", Resource: "namespaces"}: "NamespaceList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, dynObjs...)
	return &harness{
		clients: &fakeClients{dyn: dyn, disc: disc, mapper: mapper, kube: kubefake.NewClientset(kubeObjs...)},
		out:     &bytes.Buffer{},
		errOut:  &bytes.Buffer{},
	}
}

func (h *harness) app() *app {
	return &app{
		name:    defaultName,
		flags:   genericclioptions.NewConfigFlags(false),
		clients: h.clients,
		out:     h.out,
		errOut:  h.errOut,
		now:     func() time.Time { return fixedNow },
	}
}

// run executes argv (without argv[0]) and returns the exit code.
func (h *harness) run(ctx context.Context, args ...string) int {
	h.out.Reset()
	h.errOut.Reset()
	return run(ctx, append([]string{defaultName}, args...), h.app())
}

func (h *harness) mustRun(t *testing.T, args ...string) string {
	t.Helper()
	if code := h.run(context.Background(), args...); code != 0 {
		t.Fatalf("%v: exit %d, stderr: %s", args, code, h.errOut)
	}
	return h.out.String()
}

func toUnstructured(t *testing.T, obj runtime.Object, gvk schema.GroupVersionKind) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetGroupVersionKind(gvk)
	return u
}

func kustDep(wave string) apiv1.DependencyRef {
	return apiv1.DependencyRef{Name: resKust, Target: apiv1.TargetSpec{
		Group: groupKust, Kind: kindKust,
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{labelWave: wave}},
	}}
}

type ownerOpt func(*metav1.ObjectMeta, *apiv1.MilestoneStatusBase)

func recorded(ready metav1.ConditionStatus, reason string, deps ...apiv1.DependencyStatus) ownerOpt {
	return func(_ *metav1.ObjectMeta, st *apiv1.MilestoneStatusBase) {
		st.Conditions = []metav1.Condition{
			{Type: apiv1.ConditionReady, Status: ready, Reason: reason, Message: reason},
			{Type: apiv1.ConditionStalled, Status: metav1.ConditionFalse, Reason: apiv1.ReasonReconcileComplete},
		}
		st.DependsOn = deps
		for _, d := range deps {
			st.Summary.Total += d.Summary.Total
			st.Summary.Current += d.Summary.Current
		}
	}
}

func stale(meta *metav1.ObjectMeta, st *apiv1.MilestoneStatusBase) {
	meta.Generation = st.ObservedGeneration + 1
}

func applyOwnerOpts(meta *metav1.ObjectMeta, st *apiv1.MilestoneStatusBase, opts []ownerOpt) {
	meta.Generation = 2
	meta.CreationTimestamp = metav1.NewTime(fixedNow.Add(-48 * time.Hour))
	st.ObservedGeneration = 2
	st.LastEvaluatedTime = metav1.NewTime(fixedNow.Add(-42 * time.Second))
	for _, o := range opts {
		o(meta, st)
	}
}

func milestone(t *testing.T, ns, name string, deps []apiv1.DependencyRef, opts ...ownerOpt) *unstructured.Unstructured {
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       apiv1.MilestoneSpec{DependsOn: deps},
	}
	applyOwnerOpts(&m.ObjectMeta, &m.Status.MilestoneStatusBase, opts)
	return toUnstructured(t, m, apiv1.GroupVersion.WithKind("Milestone"))
}

func clusterMilestone(t *testing.T, name string, deps []apiv1.ClusterDependencyRef,
	opts ...ownerOpt) *unstructured.Unstructured {
	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       apiv1.ClusterMilestoneSpec{DependsOn: deps},
	}
	applyOwnerOpts(&cm.ObjectMeta, &cm.Status.MilestoneStatusBase, opts)
	return toUnstructured(t, cm, apiv1.GroupVersion.WithKind("ClusterMilestone"))
}

func kust(ns, name, wave string, ready bool) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(groupKust + "/v1")
	u.SetKind(kindKust)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetLabels(map[string]string{labelWave: wave})
	u.SetGeneration(1)
	_ = unstructured.SetNestedField(u.Object, int64(1), "status", "observedGeneration")
	conds := []any{map[string]any{keyType: condReady, keyStatus: "True", keyReason: "ReconciliationSucceeded"}}
	if !ready {
		conds = []any{
			map[string]any{keyType: condReady, keyStatus: "False", keyReason: "BuildFailed"},
			map[string]any{keyType: "Stalled", keyStatus: "True", keyReason: "BuildFailed", keyMessage: "build failed"},
		}
	}
	_ = unstructured.SetNestedSlice(u.Object, conds, "status", "conditions")
	return u
}

func namespace(name string, lbls map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: lbls}}
}

func deployment(ns, name, image string, lbls map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: lbls},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "manager", Image: image}},
		}}},
	}
}

// standardObjects is a small estate: two waves in flux-system (one stale),
// one Milestone elsewhere, a ClusterMilestone, and the Kustomizations they
// select.
func standardObjects(t *testing.T) []runtime.Object {
	t.Helper()
	readyDep := apiv1.DependencyStatus{
		Name: resKust, Group: groupKust, Version: "v1", Kind: kindKust, Ready: metav1.ConditionTrue,
		Reason: apiv1.ReasonAllResourcesReady, Summary: apiv1.Summary{Total: 1, Current: 1},
	}
	notReadyDep := apiv1.DependencyStatus{
		Name: resKust, Group: groupKust, Version: "v1", Kind: kindKust, Ready: metav1.ConditionFalse,
		Reason: apiv1.ReasonResourcesNotReady, Summary: apiv1.Summary{Total: 2, Current: 1, Failed: 1},
	}
	platformDep := readyDep
	platformDep.Name = "platform-kustomizations"
	return []runtime.Object{
		milestone(t, nsFlux, nameWave0, []apiv1.DependencyRef{kustDep(wave0)},
			recorded(metav1.ConditionTrue, apiv1.ReasonAllDependenciesReady, readyDep)),
		milestone(t, nsFlux, nameWave1, []apiv1.DependencyRef{kustDep(wave1)},
			recorded(metav1.ConditionFalse, apiv1.ReasonDependenciesNotReady, notReadyDep), stale),
		milestone(t, nsApps, nameAppsReady, []apiv1.DependencyRef{kustDep(wave0)},
			recorded(metav1.ConditionTrue, apiv1.ReasonAllDependenciesReady, readyDep)),
		clusterMilestone(t, namePlat, []apiv1.ClusterDependencyRef{{
			Name: "platform-kustomizations",
			Target: apiv1.ClusterTargetSpec{
				TargetSpec:        apiv1.TargetSpec{Group: groupKust, Kind: kindKust},
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "platform"}},
			},
		}}, recorded(metav1.ConditionTrue, apiv1.ReasonAllDependenciesReady, platformDep)),
		kust(nsFlux, nameInfra, wave0, true),
		kust(nsFlux, "apps", wave1, true),
		kust(nsFlux, "broken", wave1, false),
	}
}

func standardNamespaces() []runtime.Object {
	return []runtime.Object{
		namespace(nsFlux, map[string]string{"tier": "platform"}),
		namespace(nsApps, nil),
	}
}
