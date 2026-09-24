package ccache

import "sync/atomic"

// EventKind identifies which cache lifecycle transition an Event records.
type EventKind int

const (
	// EventSet marks a new item generation entering the cache.
	EventSet EventKind = iota + 1
	// EventPromotion marks an item advancing in the LRU list (either its
	// first insertion or a move towards the front).
	EventPromotion
	// EventExpiration marks the first Get that observes an item past its TTL.
	EventExpiration
	// EventDelete marks an item removed by an explicit Delete-style call.
	EventDelete
	// EventReplace marks an item displaced because the same key was overwritten.
	EventReplace
	// EventEviction marks an item removed by LRU capacity pruning.
	EventEviction
)

// String returns the stable name of the event kind.
func (k EventKind) String() string {
	switch k {
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

// IsTerminal reports whether the event kind terminates an item generation.
// Each item generation produces at most one terminal event.
func (k EventKind) IsTerminal() bool {
	switch k {
	case EventExpiration, EventDelete, EventReplace, EventEviction:
		return true
	default:
		return false
	}
}

// Event is a single lifecycle transition delivered to an EventSink.
//
// An Event intentionally carries no reference to the cached value: only the
// key, the item's generation and the kind of transition are exposed. Events
// are safe to retain after the cache moves on.
type Event struct {
	// Kind is the lifecycle transition recorded by the event. The kind is
	// also the terminal reason for the terminal event kinds.
	Kind EventKind
	// Key is the primary key for a Cache and the secondary key for a
	// LayeredCache.
	Key string
	// Group is empty for a Cache and holds the primary key for a
	// LayeredCache.
	Group string
	// Generation is the unique, monotonically increasing number of the item
	// the event refers to. Overwriting a key starts a new generation, which
	// lets an EventSink tell apart successive values that share a key.
	Generation uint64
}

// EventSink receives cache lifecycle events. A sink is opt-in via
// Configuration.EventSink.
//
// OnCacheEvent is called from a single, dedicated dispatcher goroutine per
// cache and never while the cache holds any of its internal locks, so a sink
// may call back into the cache (the call is a normal, fully reentrant cache
// operation). A sink that is too slow causes events to be dropped (see
// Configuration.EventBuffer and Cache.DroppedEvents) rather than blocking the
// cache.
type EventSink interface {
	OnCacheEvent(event Event)
}

// EventSinkFunc adapts a plain function to an EventSink.
type EventSinkFunc func(event Event)

// OnCacheEvent implements EventSink.
func (f EventSinkFunc) OnCacheEvent(event Event) {
	f(event)
}

const defaultEventBuffer = 1024

// eventMsg is the unit carried on the event channel. Regular deliveries leave
// done nil; a non-nil done channel marks a synchronization barrier which the
// dispatcher closes after all earlier messages have been delivered.
type eventMsg struct {
	event Event
	done  chan struct{}
}

// eventPipe is the single production delivery path used by both Cache and
// LayeredCache. It is only allocated when an EventSink is configured: with no
// sink the event fields of a cache stay nil and every event hook is one nil
// comparison away from being a no-op, including on the Get hot path.
type eventPipe struct {
	ch      chan eventMsg
	sink    EventSink
	done    chan struct{}
	exited  chan struct{}
	dropped int64
}

func newEventPipe(sink EventSink, buffer int) *eventPipe {
	if sink == nil {
		return nil
	}
	if buffer < 1 {
		buffer = defaultEventBuffer
	}
	p := &eventPipe{
		ch:     make(chan eventMsg, buffer),
		sink:   sink,
		done:   make(chan struct{}),
		exited: make(chan struct{}),
	}
	go p.run()
	return p
}

// emit delivers an event without blocking. If the buffer is full because the
// sink cannot keep up, the event is dropped and DroppedEvents is incremented.
// Sending a value type through a value-typed channel allocates no heap memory.
func (p *eventPipe) emit(event Event) {
	select {
	case p.ch <- eventMsg{event: event}:
	default:
		atomic.AddInt64(&p.dropped, 1)
	}
}

// sync blocks until every event enqueued before the call has been handed to
// the sink. It returns once the dispatcher has shut down.
func (p *eventPipe) sync() {
	done := make(chan struct{})
	select {
	case p.ch <- eventMsg{done: done}:
		select {
		case <-done:
		case <-p.exited:
		}
	case <-p.exited:
	}
}

// close asks the dispatcher to deliver everything still buffered and exit.
// It is invoked exactly once, from the cache worker when it stops.
func (p *eventPipe) close() {
	close(p.done)
}

func (p *eventPipe) droppedCount() int64 {
	return atomic.LoadInt64(&p.dropped)
}

func (p *eventPipe) run() {
	defer close(p.exited)
	for {
		select {
		case msg := <-p.ch:
			p.deliver(msg)
		case <-p.done:
			for {
				select {
				case msg := <-p.ch:
					p.deliver(msg)
				default:
					return
				}
			}
		}
	}
}

func (p *eventPipe) deliver(msg eventMsg) {
	if msg.done != nil {
		close(msg.done)
		return
	}
	p.sink.OnCacheEvent(msg.event)
}
