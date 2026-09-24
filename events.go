// Opt-in observability for cache lifecycle events.
//
// An EventSink receives events asynchronously, on a dedicated goroutine: no
// user code is ever invoked while an internal cache lock is held. Delivery is
// best-effort and non-blocking: if a sink is too slow and the internal event
// buffer fills up, the newest events are dropped and counted (see
// GetDroppedEvents). Events never carry a cached value, so a slow sink cannot
// retain values against the cache's wishes.
package ccache

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// EventType describes which lifecycle transition happened to an item.
type EventType uint8

const (
	EventSet EventType = iota + 1
	EventPromotion
	EventExpiration
	EventDelete
	EventReplace
	EventEviction
)

// String returns a stable, human readable name used in test output and logs.
func (e EventType) String() string {
	switch e {
	case EventSet:
		return "set"
	case EventPromotion:
		return "promotion"
	case EventExpiration:
		return "expiration"
	case EventDelete:
		return "delete"
	case EventReplace:
		return "replace"
	case EventEviction:
		return "eviction"
	default:
		return fmt.Sprintf("unknown(%d)", int(e))
	}
}

// Reason explains why an event was emitted. The same EventType can have
// different reasons (e.g. an eviction can be caused by capacity or by TTL).
type Reason uint8

const (
	// ReasonInsert: a new item was created by a set-like operation.
	ReasonInsert Reason = iota + 1
	// ReasonAccess: an existing item moved towards the front of the LRU list
	// because it was accessed.
	ReasonAccess
	// ReasonCapacity: the item was removed to free capacity.
	ReasonCapacity
	// ReasonTTL: the item had already expired when it was removed.
	ReasonTTL
	// ReasonExplicit: the caller explicitly deleted the item.
	ReasonExplicit
	// ReasonOverwritten: the item was replaced by a new value for the same key.
	ReasonOverwritten
	// ReasonPrefix: the item was removed by DeletePrefix.
	ReasonPrefix
	// ReasonFunc: the item was removed by DeleteFunc.
	ReasonFunc
)

// String returns a stable, human readable name used in test output and logs.
func (r Reason) String() string {
	switch r {
	case ReasonInsert:
		return "insert"
	case ReasonAccess:
		return "access"
	case ReasonCapacity:
		return "capacity"
	case ReasonTTL:
		return "ttl"
	case ReasonExplicit:
		return "explicit"
	case ReasonOverwritten:
		return "overwritten"
	case ReasonPrefix:
		return "prefix"
	case ReasonFunc:
		return "func"
	default:
		return fmt.Sprintf("unknown(%d)", int(r))
	}
}

// Event is a value-free snapshot of an item lifecycle transition. It carries
// the item's key, group (empty for a non-layered cache), generation and the
// reason for the transition, but never the cached value itself.
type Event struct {
	Group      string
	Key        string
	Type       EventType
	Reason     Reason
	Generation int64
}

// String renders a single event for diagnostics and failure messages.
func (e Event) String() string {
	if e.Group == "" {
		return fmt.Sprintf("%s/%s key=%s gen=%d", e.Type, e.Reason, e.Key, e.Generation)
	}
	return fmt.Sprintf("%s/%s group=%s key=%s gen=%d", e.Type, e.Reason, e.Group, e.Key, e.Generation)
}

// EventSink receives lifecycle events. Implementations must be safe for
// concurrent use. A sink is called from a single, dedicated goroutine, so a
// sink is free to reenter the cache (e.g. calling cache.Delete from OnEvent):
// such calls are only ever blocked on the public channels, never on an
// internal lock that the event delivery path itself holds. A sink must not
// call Stop or internal barrier functions from within OnEvent.
type EventSink interface {
	OnEvent(Event)
}

// The EventSinkFunc adapter allows a plain function to be used as an EventSink.
type EventSinkFunc func(Event)

// OnEvent implements EventSink.
func (f EventSinkFunc) OnEvent(event Event) {
	f(event)
}

// eventMessage is what actually travels on the internal event channel. A nil
// event is a barrier: the pump simply signals barrierCh. Barriers are the only
// way to observe causality; tests never assume a fixed goroutine schedule.
type eventMessage struct {
	event     Event
	barrierCh chan struct{}
}

type eventBus struct {
	mu      sync.RWMutex
	sink    EventSink
	events  chan eventMessage
	dropped int64 // accessed atomically; counts dropped-newest events
	closed  bool
	wg      sync.WaitGroup
}

func newEventBus(sink EventSink, buffer int) *eventBus {
	if sink == nil {
		return &eventBus{}
	}
	bus := &eventBus{
		sink:   sink,
		events: make(chan eventMessage, buffer),
	}
	bus.wg.Add(1)
	go bus.pump()
	return bus
}

// droppedCount returns the number of dropped-newest events and resets the
// counter, mirroring the cache's GetDropped contract. The read is performed
// by the cache's worker goroutine while concurrent emitters mutate the
// counter, so it is atomic; the reset loses at most drops racing with the
// read, which is acceptable for an advisory metric.
func (b *eventBus) droppedCount() int {
	if !b.enabled() {
		return 0
	}
	return int(atomic.SwapInt64(&b.dropped, 0))
}

// enabled reports whether the cache was configured with an event sink. It is
// the single hot-path check used by Get: when no sink is configured the whole
// event machinery is a nil pointer dereference-free no-op, so Get allocates
// nothing.
func (b *eventBus) enabled() bool {
	return b != nil && b.sink != nil
}

// emit hands an event to the pump without blocking. When the buffer is full
// the event is dropped-newest and counted; cache state updates are never
// blocked or rolled back because of a slow sink. emit must never be called
// while an internal bucket/list lock is held.
func (b *eventBus) emit(event Event) {
	if !b.enabled() {
		return
	}
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return
	}
	msg := eventMessage{event: event}
	select {
	case b.events <- msg:
	default:
		atomic.AddInt64(&b.dropped, 1)
	}
	b.mu.RUnlock()
}

// barrier enqueues a marker which is signalled after every event enqueued
// before it has been delivered to the sink. It reuses the production delivery
// path (same channel, same pump); it never drives event logic itself.
func (b *eventBus) barrier() {
	if !b.enabled() {
		return
	}
	ch := make(chan struct{}, 1)
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return
	}
	b.events <- eventMessage{barrierCh: ch}
	b.mu.RUnlock()
	<-ch
}

// stop marks the bus closed, drains every buffered event into the sink and
// waits for the pump to exit. Events dropped because of a full buffer are
// already counted. Stop is invoked once, from the cache's worker after it has
// drained its own queues, so all events that could be produced have been
// enqueued.
func (b *eventBus) stop() {
	if !b.enabled() {
		return
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.wg.Wait()
		return
	}
	b.closed = true
	close(b.events)
	b.mu.Unlock()
	b.wg.Wait()
}

func (b *eventBus) pump() {
	defer b.wg.Done()
	for msg := range b.events {
		if msg.barrierCh != nil {
			msg.barrierCh <- struct{}{}
			continue
		}
		b.sink.OnEvent(msg.event)
	}
}
