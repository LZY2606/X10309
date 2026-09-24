package ccache

import "sync/atomic"

// EventType categorizes an Event emitted by the cache.
type EventType uint8

const (
	// EventSet is emitted when an item enters the cache. Item lifetimes
	// always start with an EventSet.
	EventSet EventType = iota + 1
	// EventPromotion is emitted when an item joins or moves within the LRU list.
	EventPromotion
	// EventExpiration is emitted when an expired item is reaped.
	EventExpiration
	// EventDelete is emitted when an item is explicitly removed.
	EventDelete
	// EventReplace is emitted when an item is overwritten by a newer generation.
	EventReplace
	// EventEviction is emitted when an item is removed under memory pressure.
	EventEviction
)

// EventReason describes why an event happened. Every EventType is produced
// with a single associated reason.
type EventReason uint8

const (
	ReasonCreated     EventReason = iota + 1 // EventSet: a new key was inserted
	ReasonReplaced                           // EventSet: the key overwrote an existing generation
	ReasonInserted                           // EventPromotion: first promotion into the LRU list
	ReasonAccessed                           // EventPromotion: re-promotion caused by Gets
	ReasonDeleted                            // EventDelete: Delete/DeletePrefix/DeleteFunc
	ReasonOverwritten                        // EventReplace: replaced by a new Set
	ReasonExpired                            // EventExpiration: reaped after its TTL elapsed
	ReasonCapacity                           // EventEviction: automatic LRU eviction
	ReasonPruned                             // EventEviction: explicit GC/SetMaxSize pruning
)

// Event is a value-free description of a cache lifecycle transition. It never
// carries the cached value: a sink must not be able to observe (or retain)
// user data through events, and events stay small enough to be allocated on
// any cache operation without meaningful overhead.
type Event struct {
	// Generation is the per-key generation of the item the event belongs to.
	// The first generation of a key is 1 and each successful Set of the key
	// increments it.
	Generation int64
	Reason     EventReason
	Type       EventType
	reserved   uint8
	// Key is the secondary key for a regular cache and a LayeredCache.
	Key string
	// Group is the primary key of a LayeredCache entry. It is empty for a
	// regular cache.
	Group string

	// barrier is used internally by eventHub.barrier; it is nil for every
	// event handed to a sink.
	barrier chan struct{}
}

// EventSink receives lifecycle events. Sinks are invoked on a dedicated
// goroutine: user code is never called while an internal cache lock is held.
// Sinks must be safe to call concurrently with cache operations; a single sink
// sees events from one goroutine at a time.
type EventSink interface {
	OnEvent(Event)
}

// EventSinkFunc adapts a plain function to the EventSink interface.
type EventSinkFunc func(Event)

func (f EventSinkFunc) OnEvent(event Event) {
	f(event)
}

// eventHub decouples event production (the cache worker) from event
// consumption (the user-provided sink). Emission is non-blocking: when the
// bounded buffer is full the event is dropped and counted, so a slow sink can
// never stall cache operations. All dispatch happens on its own goroutine,
// outside of every bucket lock.
type eventHub struct {
	sink    EventSink
	events  chan Event
	stop    chan struct{}
	stopped chan struct{}
	dropped int64
}

func newEventHub(sink EventSink, buffer int) *eventHub {
	if buffer < 1 {
		buffer = 1
	}
	hub := &eventHub{
		sink:    sink,
		events:  make(chan Event, buffer),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go hub.dispatch()
	return hub
}

// emit hands one event to the sink. It never blocks, never invokes the sink
// directly and takes no cache lock. Returns false when the event had to be
// dropped because the sink is too slow or the hub is shutting down.
func (h *eventHub) emit(event Event) bool {
	if h == nil || h.sink == nil {
		return true
	}
	select {
	case h.events <- event:
		return true
	default:
		select {
		case <-h.stop:
		default:
			atomic.AddInt64(&h.dropped, 1)
		}
		return false
	}
}

// barrier unblocks once every event enqueued before it has been handed to the
// sink. It exists for deterministic testing of event causality.
func (h *eventHub) barrier() {
	if h == nil || h.sink == nil {
		return
	}
	done := make(chan struct{})
	h.events <- Event{Type: 0, reserved: eventBarrier, barrier: done}
	<-done
}

// shutdown drains the buffered events and waits for the dispatcher to exit.
// Events already causally produced when shutdown starts are all delivered.
func (h *eventHub) shutdown() {
	if h == nil || h.sink == nil {
		return
	}
	close(h.stop)
	<-h.stopped
}

// DroppedEvents returns how many events were discarded due to a back-pressured
// sink since the cache was created. The counter is monotonic.
func (h *eventHub) droppedEvents() int64 {
	if h == nil {
		return 0
	}
	return atomic.LoadInt64(&h.dropped)
}

func (h *eventHub) dispatch() {
	defer close(h.stopped)
	for {
		select {
		case event := <-h.events:
			if event.reserved == eventBarrier {
				close(event.barrier)
				continue
			}
			h.sink.OnEvent(event)
		case <-h.stop:
			for {
				select {
				case event := <-h.events:
					if event.reserved == eventBarrier {
						close(event.barrier)
						continue
					}
					h.sink.OnEvent(event)
				default:
					return
				}
			}
		}
	}
}

const eventBarrier uint8 = 255

// terminalEvent maps the reason recorded on a terminated item to the event
// describing that termination.
func terminalEvent(reason EventReason) (EventType, bool) {
	switch reason {
	case ReasonDeleted:
		return EventDelete, true
	case ReasonOverwritten:
		return EventReplace, true
	case ReasonExpired:
		return EventExpiration, true
	case ReasonCapacity, ReasonPruned:
		return EventEviction, true
	default:
		return 0, false
	}
}
