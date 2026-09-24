package ccache

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/karlseguin/ccache/v3/assert"
)

// recordingSink is a minimal EventSink used across the event contract tests.
// It records exactly what the production dispatcher hands to it, in delivery
// order.
type recordingSink struct {
	mu     sync.Mutex
	events []Event
}

func newRecordingSink() *recordingSink {
	return &recordingSink{}
}

func (s *recordingSink) OnCacheEvent(event Event) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
}

func (s *recordingSink) snapshot() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// settle is the event-causality barrier: after it returns, every operation the
// calling goroutine issued has reached both the cache worker and the event
// dispatcher. It relies on the cache's own barriers (SyncUpdates, SyncEvents)
// and never forces a goroutine schedule.
func settle[T any](t *testing.T, c *Cache[T]) []Event {
	t.Helper()
	c.SyncUpdates()
	c.SyncEvents()
	sink, ok := c.Configuration.eventSink.(*recordingSink)
	if !ok {
		t.Fatalf("contract setup violated: cache is not wired to a recordingSink")
	}
	return sink.snapshot()
}

// settleLayered is settle for a LayeredCache.
func settleLayered[T any](t *testing.T, c *LayeredCache[T]) []Event {
	t.Helper()
	c.SyncUpdates()
	c.SyncEvents()
	sink, ok := c.Configuration.eventSink.(*recordingSink)
	if !ok {
		t.Fatalf("contract setup violated: cache is not wired to a recordingSink")
	}
	return sink.snapshot()
}

// traceViolation reports the first place in the trace where a causal invariant
// fails, together with the offending event and its immediate neighbors. The
// message is keyed by event identity instead of a fixed whole-trace dump.
func traceViolation(trace []Event, index int, format string, args ...interface{}) string {
	at := Event{}
	if index >= 0 && index < len(trace) {
		at = trace[index]
	}
	from := index - 1
	if from < 0 {
		from = 0
	}
	to := index + 2
	if to > len(trace) {
		to = len(trace)
	}
	msg := fmtSprintf(format, args...)
	return msg + fmtSprintf("; first violation at trace[%d]=%s, window=%v (trace length %d)",
		index, formatEvent(at), trace[from:to], len(trace))
}

func formatEvent(e Event) string {
	if e.Group == "" {
		return fmtSprintf("Event(%s key=%s gen=%d)", e.Kind, e.Key, e.Generation)
	}
	return fmtSprintf("Event(%s group=%s key=%s gen=%d)", e.Kind, e.Group, e.Key, e.Generation)
}

// fmtSprintf is a tiny indirection kept so helpers read like regular Sprintf
// calls without every file needing the import at use sites.
func fmtSprintf(format string, args ...interface{}) string {
	return sprintfImpl(format, args...)
}

// checkCausalTrace validates scheduling-independent causality of a trace:
//  1. a set event is the first event of every item generation;
//  2. each generation has exactly one set event;
//  3. each generation has at most one terminal event, and it is last;
//  4. promotion events only occur for live (non-terminated) generations;
//  5. generations of a key are created in strictly increasing order.
//
// It returns an empty string when the trace holds, otherwise a message that
// pinpoints the first violating position.
func checkCausalTrace(trace []Event) string {
	type genState struct {
		seenSet bool
		terminal bool
	}
	gens := make(map[uint64]*genState)
	keyGen := make(map[string]uint64)
	for i, e := range trace {
		st := gens[e.Generation]
		if st == nil {
			if e.Kind != EventSet {
				return traceViolation(trace, i, "generation %d begins with %s instead of set", e.Generation, e.Kind)
			}
			st = &genState{seenSet: true}
			gens[e.Generation] = st
			if prev, exists := keyGen[e.Key]; exists && e.Generation <= prev {
				return traceViolation(trace, i, "key %s regressed from generation %d to %d", e.Key, prev, e.Generation)
			}
			keyGen[e.Key] = e.Generation
			continue
		}
		if e.Kind == EventSet {
			return traceViolation(trace, i, "generation %d has a second set event", e.Generation)
		}
		if st.terminal {
			return traceViolation(trace, i, "generation %d has an event after its terminal event", e.Generation)
		}
		if e.Kind.IsTerminal() {
			st.terminal = true
		}
	}
	return ""
}

func eventConfig[T any]() *Configuration[T] {
	return Configure[T]().EventSink(newRecordingSink())
}

func eventKinds(events []Event, group string) []EventKind {
	out := make([]EventKind, 0, len(events))
	for _, e := range events {
		if e.Group == group {
			out = append(out, e.Kind)
		}
	}
	return out
}

func findEvent(events []Event, kind EventKind, key string) (Event, int) {
	for i, e := range events {
		if e.Kind == kind && e.Key == key {
			return e, i
		}
	}
	return Event{}, -1
}

func terminalEvents(events []Event, key string) []Event {
	var out []Event
	for _, e := range events {
		if e.Key == key && e.Kind.IsTerminal() {
			out = append(out, e)
		}
}
	return out
}

// Test_EventSink_Disabled_GetDoesNotAllocate guards the hot-path contract:
// with the feature opted out, Get performs no event-related allocation and no
// event dispatcher exists.
func Test_EventSink_Disabled_GetDoesNotAllocate(t *testing.T) {
	c := New(Configure[string]())
	defer c.Stop()
	c.Set("spice", "flow", time.Minute)
	c.SyncUpdates()

	allocs := testing.AllocsPerRun(1000, func() {
		_ = c.Get("spice")
	})
	assert.Equal(t, allocs, float64(0))
	assert.Equal(t, c.DroppedEvents(), int64(0))
	c.SyncEvents() // no-op must be safe without a sink
}

var _ = errors.New
var _ = reflect.TypeOf

