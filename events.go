package ccache

import "sync/atomic"

// EventKind identifies why an Event was emitted.
type EventKind int

const (
	// A new item was written to the cache (Set, TrackingSet, Setnx, Setnx2,
	// Replace on an existing key, or a successful Fetch).
	EventSet EventKind = iota + 1
	// An item was moved to the front of the LRU list because it was used
	// enough (see GetsPerPromote).
	EventPromotion
	// A Get observed an item whose TTL had already elapsed. The item is not
	// removed by this; it is simply the first time the expiry was noticed.
	EventExpiration
	// An item was explicitly removed (Delete, DeletePrefix, DeleteFunc).
	EventDelete
	// An item was dropped from the cache because a new value was set for
	// its key.
	EventReplace
	// An item was pruned because the cache grew beyond its max size.
	EventEviction
)

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
	}
	return "unknown"
}

// terminal reports whether the event ends the lifecycle of an item
// generation. At most one terminal event is ever emitted per item.
func (k EventKind) terminal() bool {
	switch k {
	case EventExpiration, EventDelete, EventReplace, EventEviction:
		return true
	}
	return false
}

// Event describes a single cache lifecycle event, delivered to the sink
// registered with Configuration.OnEvent.
//
// An Event deliberately does not carry the item's value: it is a lightweight
// record (kind, key and item generation) meant for replaying cache behavior.
// The generation is a process-wide monotonically increasing identifier
// assigned to every item instance; each write to a key produces a new item
// with a higher generation, so generations can be used to correlate events
// that refer to the same logical item instance.
type Event struct {
	Kind       EventKind
	Key        string
	Generation uint64
}

// emitEvent enqueues an event for asynchronous delivery to the configured
// sink. It is a no-op when no sink is configured, which keeps the event
// plumbing allocation-free on the hot path (e.g. Get). Delivery is
// best-effort: if the event queue is full (slow sink) or the cache has been
// stopped, the event is dropped and counted in DroppedEvents.
func (c *Cache[T]) emitEvent(kind EventKind, item *Item[T]) {
	if c.events == nil {
		return
	}
	if atomic.LoadInt32(&c.eventsClosed) == 1 {
		atomic.AddInt64(&c.droppedEvents, 1)
		return
	}
	select {
	case c.events <- Event{Kind: kind, Key: item.key, Generation: item.generation}:
	default:
		atomic.AddInt64(&c.droppedEvents, 1)
	}
}

// emitTerminal emits a lifecycle-ending event, guaranteeing that each item
// produces at most one terminal event no matter how many code paths
// (expiry notice, explicit delete, replace, eviction) race to retire it.
func (c *Cache[T]) emitTerminal(kind EventKind, item *Item[T]) {
	if c.events == nil {
		return
	}
	if !atomic.CompareAndSwapInt32(&item.terminal, 0, 1) {
		return
	}
	c.emitEvent(kind, item)
}

// dispatchEvents is the only place user code (the sink) is invoked from.
// It runs on its own goroutine and never holds a cache lock, so a sink is
// free to call back into the cache. On Stop, remaining queued events are
// drained before the dispatcher exits.
func (c *Cache[T]) dispatchEvents() {
	for {
		select {
		case event := <-c.events:
			c.onEvent(event)
		case <-c.eventsDone:
			for {
				select {
				case event := <-c.events:
					c.onEvent(event)
				default:
					return
				}
			}
		}
	}
}
