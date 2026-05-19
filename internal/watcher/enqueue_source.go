/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher

import (
	"context"
	"sync"

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// EnqueueSource is a controller-runtime source.Source that exposes a typed
// Enqueue method for direct workqueue insertion. controller-runtime calls
// Start exactly once when the controller is constructed, passing the
// controller's own workqueue; from that point Enqueue forwards reconcile
// requests straight to the queue without an intermediate channel.
//
// This replaces the older `chan event.GenericEvent` + source.Channel
// bridge: with the channel, a slow consumer could fill the buffer and
// block every producer goroutine (informer dispatchers, the CRD watcher);
// with the workqueue, Add is non-blocking and key-deduped, so an event
// storm for the same owner collapses to a single reconcile request.
//
// Enqueue is a no-op when the source has not yet been Started or after the
// manager has signalled shutdown via ctx.Done. Events lost in those
// windows are recoverable by the informer's periodic resync and the
// stalled-requeue safety net.
type EnqueueSource struct {
	mu    sync.RWMutex
	queue workqueue.TypedRateLimitingInterface[reconcile.Request]
}

// NewEnqueueSource returns a source with no queue captured yet.
func NewEnqueueSource() *EnqueueSource {
	return &EnqueueSource{}
}

// Start implements source.TypedSource[reconcile.Request]. Captures the
// controller's workqueue and returns immediately. A background goroutine
// clears the captured queue when ctx is cancelled so post-shutdown
// Enqueues become no-ops instead of pushing to a closed queue.
func (s *EnqueueSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	s.mu.Lock()
	s.queue = queue
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		s.queue = nil
		s.mu.Unlock()
	}()
	return nil
}

// Enqueue forwards req to the captured workqueue. Safe to call from any
// goroutine. No-op when the source has not yet been Started or after the
// manager has shut down.
func (s *EnqueueSource) Enqueue(req reconcile.Request) {
	s.mu.RLock()
	q := s.queue
	s.mu.RUnlock()
	if q == nil {
		return
	}
	q.Add(req)
}

// Verify the interface fit at compile time.
var _ source.TypedSource[reconcile.Request] = (*EnqueueSource)(nil)
