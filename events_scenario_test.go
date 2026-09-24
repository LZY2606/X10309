package ccache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scenario 1: capacity eviction.
// Overfilling a size-5 cache with PercentToPrune(20) evicts the least recently
// used generation; the eviction event carries the evicted generation and the
// capacity reason.
func Test_Events_CapacityEvictionTrajectory(t *testing.T) {
	rec := newEventRecorder()
	cache := New(Configure[any]().MaxSize(5).PercentToPrune(20).Events(rec))
	defer cache.Stop()

	for i := 0; i < 7; i++ {
		cache.Set(keyN(i), i, time.Minute)
	}
	rec.barrier(cache)

	// Caller-visible results: two oldest keys are gone.
	if item := cache.Get("k0"); item != nil {
		t.Fatalf("first violation: k0 should have been capacity-evicted, got item gen %d", item.Generation())
	}
	if item := cache.Get("k1"); item != nil {
		t.Fatalf("first violation: k1 should have been capacity-evicted, got item gen %d", item.Generation())
	}
	if got := cache.ItemCount(); got != 5 {
		t.Fatalf("first violation: expected 5 survivors, ItemCount=%d", got)
	}
	if got := cache.GetDropped(); got != 2 {
		t.Fatalf("first violation: GetDropped counter=%d, want 2 (counter unchanged by event sink)", got)
	}
	if got := cache.DroppedEvents(); got != 0 {
		t.Fatalf("first violation: DroppedEvents=%d, want 0 under a caught-up sink", got)
	}

	events := rec.snapshot()
	expected := []expectedEvent{
		{key: terminalEventKey(EventEviction, "k0", 1, ReasonCapacity), count: 1},
		{key: terminalEventKey(EventEviction, "k1", 1, ReasonCapacity), count: 1},
	}
	assertTrace(t, events, expected)

	// Invariant: at most one terminal event per item generation.
	for k, n := range terminalGroups(events) {
		if n > 1 {
			t.Fatalf("first violation: item %s has %d terminal events, want at most 1", k, n)
		}
	}
}

// Scenario 2: TTL expiration.
// An expired generation swept during pruning is reported as an expiration,
// while live generations pruned in the same sweep are evictions.
func Test_Events_TTLExpirationTrajectory(t *testing.T) {
	rec := newEventRecorder()
	cache := New(Configure[any]().MaxSize(10).PercentToPrune(100).Events(rec))
	defer cache.Stop()

	cache.Set("live1", 1, time.Minute)
	cache.Set("dead1", 2, -time.Minute)
	cache.Set("live2", 3, time.Minute)
	cache.Set("dead2", 4, -time.Minute)
	cache.SyncUpdates()

	cache.GC()
	rec.barrier(cache)

	if cache.GetWithoutPromote("dead1") != nil || cache.GetWithoutPromote("dead2") != nil {
		t.Fatal("first violation: expired generations must be removed from the lookup")
	}
	if cache.GetWithoutPromote("live1") == nil || cache.GetWithoutPromote("live2") == nil {
		t.Fatal("first violation: live generations must survive the prune sweep")
	}

	events := rec.snapshot()
	expected := []expectedEvent{
		{key: terminalEventKey(EventExpiration, "dead1", 1, ReasonExpired), count: 1},
		{key: terminalEventKey(EventExpiration, "dead2", 1, ReasonExpired), count: 1},
	}
	assertTrace(t, events, expected)
	for _, name := range []string{"live1", "live2"} {
		for _, event := range events {
			if event.Key == name && isTerminal(event.Type) {
				t.Fatalf("first violation: live key %s got a terminal event %s", name, keyOf(event))
			}
		}
	}
}

func isTerminal(t EventType) bool {
	return t == EventDelete || t == EventReplace || t == EventExpiration || t == EventEviction
}

// Scenario 3: explicit Delete emits a single delete terminal event, and the
// event order for the generation is set -> promotion -> delete (verified with
// a barrier, not sleep).
func Test_Events_ExplicitDeleteTrajectory(t *testing.T) {
	rec := newEventRecorder()
	cache := New(Configure[any]().Events(rec))
	defer cache.Stop()

	cache.Set("k", "v", time.Minute)
	if !cache.Delete("k") {
		t.Fatal("first violation: Delete returned false for a present key")
	}
	if cache.Delete("k") {
		t.Fatal("first violation: deleting an absent key must return false")
	}
	rec.barrier(cache)

	if cache.Get("k") != nil {
		t.Fatal("first violation: deleted key is still visible")
	}

	events := rec.snapshot()
	del := terminalEventKey(EventDelete, "k", 1, ReasonDeleted)
	expected := []expectedEvent{
		{key: setEvent("k", 1, ReasonCreated), count: 1, before: []eventKey{
			promotionEvent("k", 1, ReasonInserted), del,
		}},
		{key: promotionEvent("k", 1, ReasonInserted), count: 1, before: []eventKey{del}},
		{key: del, count: 1},
	}
	assertTrace(t, events, expected)
}

// Scenario 4: overwrite. The replaced generation emits a replace terminal
// event; the new generation starts a fresh lifecycle with its own generation
// number.
func Test_Events_OverwriteTrajectory(t *testing.T) {
	rec := newEventRecorder()
	cache := New(Configure[any]().GetsPerPromote(1).Events(rec))
	defer cache.Stop()

	cache.Set("k", "v1", time.Minute)
	cache.Set("k", "v2", time.Minute)
	cache.Set("k", "v3", time.Minute)
	if item := cache.Get("k"); item == nil || item.Value() != "v3" || item.Generation() != 3 {
		t.Fatalf("first violation: caller must observe generation 3/value v3, got %v", item)
	}
	rec.barrier(cache)

	events := rec.snapshot()
	expected := []expectedEvent{
		{key: setEvent("k", 1, ReasonCreated), count: 1},
		{key: terminalEventKey(EventReplace, "k", 1, ReasonOverwritten), count: 1},
		{key: setEvent("k", 2, ReasonReplaced), count: 1},
		{key: terminalEventKey(EventReplace, "k", 2, ReasonOverwritten), count: 1},
		{key: setEvent("k", 3, ReasonReplaced), count: 1},
	}
	assertTrace(t, events, expected)

	// No terminal event may reference the live generation yet.
	for _, event := range events {
		if event.Key == "k" && event.Generation == 3 && isTerminal(event.Type) {
			t.Fatalf("first violation: live generation 3 received terminal event %s", keyOf(event))
		}
	}
}

// Scenario 5: promotion queue congestion.
// PromoteBuffer(1) makes the bounded promotion channel demonstrably congest
// while the worker is held in an OnDelete callback (a public configuration
// hook; no internal lock is held there). Gets must never block on the
// congested promotion path, and cache contents stay consistent.
func Test_Events_PromotionQueueCongestion(t *testing.T) {
	releaseWorker := make(chan struct{})
	enteredCallback := make(chan struct{})
	cache := New(Configure[any]().
		PromoteBuffer(1).
		GetsPerPromote(1).
		OnDelete(func(*Item[any]) {
			close(enteredCallback)
			<-releaseWorker
		}).
		Events(newEventRecorder()))
	defer func() {
		close(releaseWorker)
		cache.Stop()
	}()

	cache.Set("victim", 1, time.Minute)
	cache.SyncUpdates()
	cache.Delete("victim")
	<-enteredCallback // worker is now parked with the deletables channel occupied

	// A full promotion queue would make a blocking send stall; Get uses a
	// non-blocking send, so all Gets return promptly while congested.
	for i := 0; i < 200; i++ {
		cache.Set("hot", i, time.Minute)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 500; i++ {
			if item := cache.Get("hot"); item == nil {
				t.Error("first violation: Get returned nil while promotion channel was congested")
				return
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("first violation: Get blocked on the congested promotion queue")
	}

	close(releaseWorker)
	cache.SyncUpdates()
	if item := cache.Get("hot"); item == nil {
		t.Fatal("first violation: hot item missing after congestion drained")
	}
}

// Scenario 6a: slow sink. Events are dropped with an explicit policy: emission
// is non-blocking, dropped events are counted in DroppedEvents, and the cache
// never waits for the sink.
func Test_Events_SlowSinkDropsAndCounts(t *testing.T) {
	block := make(chan struct{})
	var once sync.Once
	rec := newEventRecorder()
	sink := EventSinkFunc(func(event Event) {
		rec.OnEvent(event)
		once.Do(func() { <-block })
	})
	cache := New(Configure[any]().EventBuffer(2).Events(sink))
	defer func() {
		close(block)
		cache.Stop()
	}()

	completed := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			cache.Set(keyN(i), i, time.Minute)
		}
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("first violation: cache Set path blocked on a slow event sink")
	}

	close(block)
	cache.SyncUpdates()
	cache.events.barrier()

	delivered := int64(rec.len())
	dropped := cache.DroppedEvents()
	if dropped == 0 {
		t.Fatal("first violation: a saturated sink must record dropped events in DroppedEvents, got 0")
	}
	if delivered+dropped != 200 {
		t.Fatalf("first violation: accounting mismatch: delivered=%d dropped=%d, want total 200 (explicit drop policy must conserve events)", delivered, dropped)
	}
}

// Scenario 6b: sink reentrancy. The sink calls back into the same cache
// (Set/Delete/Get) from OnEvent. This must not deadlock: user code never runs
// under a bucket lock.
func Test_Events_SinkReentersCacheSafely(t *testing.T) {
	rec := newEventRecorder()
	var reentered int64
	sink := EventSinkFunc(func(event Event) {
		rec.OnEvent(event)
		if event.Type == EventSet && event.Key == "trigger" {
			// Calling back into the cache while delivering an event.
			cacheRef.Set("sink-wrote", "ok", time.Minute)
			if item := cacheRef.Get("sink-wrote"); item == nil || item.Value() != "ok" {
				return
			}
			cacheRef.Delete("sink-wrote")
			atomic.AddInt64(&reentered, 1)
		}
	})
	cache := New(Configure[any]().EventBuffer(16).Events(sink))
	cacheRef = cache
	defer cache.Stop()

	cache.Set("trigger", "go", time.Minute)

	done := make(chan struct{})
	go func() {
		cache.SyncUpdates()
		cache.events.barrier()
		// Give the reentrant writes one more drain round.
		cache.SyncUpdates()
		cache.events.barrier()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("first violation: sink reentrancy deadlocked; user code must not run under internal locks")
	}
	if atomic.LoadInt64(&reentered) != 1 {
		t.Fatalf("first violation: reentrant sink body did not complete, reentered=%d", reentered)
	}
}

var cacheRef *Cache[any]

// Scenario 7: shutdown. Stop drains in-flight state and the event queue: every
// event causally produced before Stop is delivered, and each terminal item is
// terminated at most once.
func Test_Events_CloseDrainsTrajectory(t *testing.T) {
	rec := newEventRecorder()
	cache := New(Configure[any]().MaxSize(4).PercentToPrune(50).Events(rec))

	for i := 0; i < 8; i++ {
		cache.Set(keyN(i), i, time.Minute)
	}
	cache.Set("gone", 1, time.Minute)
	cache.Delete("gone")
	cache.Stop()

	events := rec.snapshot()

	// Every set produced an event before shutdown (sets are emitted by the
	// caller before Stop could observe them); terminal items at most once.
	setCount := 0
	for _, event := range events {
		if event.Type == EventSet {
			setCount++
		}
	}
	if setCount != 9 {
		t.Fatalf("first violation: Stop lost causally-prior events: delivered %d/9 set events; trace:%s", setCount, describeTrace(events))
	}
	for k, n := range terminalGroups(events) {
		if n > 1 {
			t.Fatalf("first violation after close: item %s terminated %d times", k, n)
		}
	}
}
