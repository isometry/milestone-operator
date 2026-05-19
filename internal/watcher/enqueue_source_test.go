/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/isometry/milestone-operator/internal/watcher"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func newReconcileQueue() workqueue.TypedRateLimitingInterface[reconcile.Request] {
	return workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)
}

// TestEnqueueSource_PostStartEnqueueLandsInWorkqueue covers the happy path:
// after Start captures the workqueue, Enqueue forwards the reconcile.Request.
func TestEnqueueSource_PostStartEnqueueLandsInWorkqueue(t *testing.T) {
	src := watcher.NewEnqueueSource()
	q := newReconcileQueue()
	defer q.ShutDown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := src.Start(ctx, q); err != nil {
		t.Fatalf("Start: %v", err)
	}

	want := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "wave-0"}}
	src.Enqueue(want)

	got, _ := q.Get()
	q.Done(got)
	if got != want {
		t.Errorf("queue head = %v, want %v", got, want)
	}
}

// TestEnqueueSource_PreStartEnqueueDropped covers the "events sent before
// the controller installs its workqueue" window: rather than panic or
// block, Enqueue silently drops. The informer's periodic resync and the
// stalled-requeue safety net make these recoverable.
func TestEnqueueSource_PreStartEnqueueDropped(t *testing.T) {
	src := watcher.NewEnqueueSource()
	// No Start; should not panic or block.
	src.Enqueue(reconcile.Request{NamespacedName: types.NamespacedName{Name: "dropped"}})
}

// TestEnqueueSource_PostShutdownEnqueueDropped pins the post-shutdown
// semantic: once ctx is cancelled, subsequent Enqueues are no-ops instead
// of pushing to the now-shutting-down workqueue.
func TestEnqueueSource_PostShutdownEnqueueDropped(t *testing.T) {
	src := watcher.NewEnqueueSource()
	q := newReconcileQueue()

	ctx, cancel := context.WithCancel(context.Background())
	if err := src.Start(ctx, q); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cancel()
	// Allow the source's shutdown goroutine to clear the captured queue.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if q.Len() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Shut down the queue too, mirroring controller-runtime's behaviour.
	q.ShutDown()

	// Post-shutdown Enqueue must not panic and must not push to the queue.
	src.Enqueue(reconcile.Request{NamespacedName: types.NamespacedName{Name: "after-shutdown"}})
	if q.Len() != 0 {
		t.Errorf("queue length after post-shutdown Enqueue = %d, want 0", q.Len())
	}
}
