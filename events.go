package ccache

import (
	"sync/atomic"
	"time"
)

// EventType identifies which lifecycle event happened to a cache item.
type EventType uint8

const (
	// EventSet is emitted whenever a new generation is stored for a key.
	EventSet EventType = iota + 1
	// EventPromotion is emitted when an item advances through the LRU machinery.
	EventPromotion
	// EventExpiration is the terminal event emitted when an item is removed
	// because its TTL elapsed.
	EventExpiration
	// EventDelete is the terminal event emitted for an explicit deletion.
	EventDelete
	// EventReplace is the terminal event emitted when an item is overwritten
	// by a newer generation of the same key.
	EventReplace
	// EventEviction is the terminal event emitted when an item is evicted
	// because the cache exceeded its capacity.
	EventEviction
)

func (t EventType) String() string {
	switch t {
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
		return "unknown"
	}
}

// Reason explains why an event was emitted.
type Reason uint8

const (
	// ReasonInsert accompanies EventSet when a previously absent key is stored.
	ReasonInsert Reason = iota + 1
	// ReasonOverwritten accompanies EventSet when an existing generation was
	// replaced.
	ReasonOverwritten
	// ReasonQueued accompanies EventPromotion when an item enters the LRU list
	// for the first time.
	ReasonQueued
	// ReasonMoved accompanies EventPromotion when an item already in the LRU
	// list is moved towards the front.
	ReasonMoved
	// ReasonQueueFull accompanies EventPromotion when a promotion had to be
	// skipped because the promotion queue was congested.
	ReasonQueueFull
	// ReasonTTL accompanies an EventExpiration.
	ReasonTTL
	// ReasonExplicit accompanies an EventDelete caused by a caller deletion.
	ReasonExplicit
	// ReasonReplaced accompanies an EventReplace.
	ReasonReplaced
	// ReasonCapacity accompanies an EventEviction caused by capacity pressure.
	ReasonCapacity
)

func (r Reason) String() string {
	switch r {
	case ReasonInsert:
		return "insert"
	case ReasonOverwritten:
		return "overwritten"
	case ReasonQueued:
		return "queued"
	case ReasonMoved:
		return "moved"
	case ReasonQueueFull:
		return "promotion-queue-full"
	case ReasonTTL:
		return "ttl-expired"
	case ReasonExplicit:
		return "explicit-delete"
	case ReasonReplaced:
		return "replaced"
	case ReasonCapacity:
		return "capacity"
	default:
		return "unknown"
	}
}

// terminal reports whether the event type removes a generation for good.
func (t EventType) terminal() bool {
	switch t {
	case EventExpiration, EventDelete, EventReplace, EventEviction:
		return true
	}
	return false
}

// Event describes a single item lifecycle transition. An Event never carries
// the cached value: observing the trace must not extend the lifetime of cached
// data nor expose it to subscribers.
type Event struct {
	Type       EventType
	Reason     Reason
	Key        string
	Group      string
	Generation int64
	Size       int64
	At         time.Time
}

// EventSink receives cache lifecycle events. Implementations must be safe for
// concurrent use. Sinks are called from a dedicated goroutine, never while a
// cache bucket lock is held. A slow sink never blocks cache operations: once
// the event buffer is full, further events are dropped (see
// Cache.GetDroppedEvents).
type EventSink interface {
	OnCacheEvent(Event)
}

// EventSinkFunc adapts a plain function into an EventSink.
type EventSinkFunc func(Event)

func (f EventSinkFunc) OnCacheEvent(event Event) {
	f(event)
}

// generationCounter hands out dense, per-cache item generations.
type generationCounter struct {
	next int64
}

func (g *generationCounter) generation() int64 {
	return atomic.AddInt64(&g.next, 1)
}

// eventHub decouples cache goroutines from the user supplied sink. All sends
// are non-blocking, so user code (including reentrant cache calls) can never
// stall the cache; overflow is counted instead.
type eventHub struct {
	sink    EventSink
	events  chan Event
	dropped int64
	stop    chan struct{}
	done    chan struct{}
}

func newEventHub(sink EventSink, buffer int) *eventHub {
	if sink == nil {
		return nil
	}
	if buffer <= 0 {
		buffer = 1024
	}
	hub := &eventHub{
		sink:   sink,
		events: make(chan Event, buffer),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go hub.run()
	return hub
}

func (h *eventHub) emit(event Event) {
	if h == nil {
		return
	}
	select {
	case h.events <- event:
	default:
		atomic.AddInt64(&h.dropped, 1)
	}
}

func (h *eventHub) droppedCount() int64 {
	if h == nil {
		return 0
	}
	return atomic.LoadInt64(&h.dropped)
}

func (h *eventHub) run() {
	defer close(h.done)
	for {
		select {
		case event := <-h.events:
			h.sink.OnCacheEvent(event)
		case <-h.stop:
			for {
				select {
				case event := <-h.events:
					h.sink.OnCacheEvent(event)
				default:
					return
				}
			}
		}
	}
}

// close stops the delivery goroutine. Queued events are delivered best effort
// before it exits.
func (h *eventHub) close() {
	if h == nil {
		return
	}
	close(h.stop)
	<-h.done
}
