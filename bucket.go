package ccache

import (
	"strings"
	"sync"
	"time"
)

type bucket[T any] struct {
	sync.RWMutex
	lookup map[string]*Item[T]
	nextGen func() int64
}

func (b *bucket[T]) itemCount() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.lookup)
}

func (b *bucket[T]) forEachFunc(matches func(key string, item *Item[T]) bool) bool {
	lookup := b.lookup
	b.RLock()
	defer b.RUnlock()
	for key, item := range lookup {
		if !matches(key, item) {
			return false
		}
	}
	return true
}

func (b *bucket[T]) get(key string) *Item[T] {
	b.RLock()
	defer b.RUnlock()
	return b.lookup[key]
}

func (b *bucket[T]) setnx(key string, value T, duration time.Duration, track bool) (*Item[T], bool) {
	b.RLock()
	item := b.lookup[key]
	b.RUnlock()
	if item != nil {
		return item, false
	}

	b.Lock()
	defer b.Unlock()

	// check again under write lock
	item = b.lookup[key]
	if item != nil {
		return item, false
	}

	expires := time.Now().Add(duration).UnixNano()
	newItem := newItem(key, value, expires, b.generation(), track)
	b.lookup[key] = newItem
	return newItem, true
}

func (b *bucket[T]) setnx2(key string, f func() T, duration time.Duration, track bool) (*Item[T], bool) {
	b.RLock()
	item := b.lookup[key]
	b.RUnlock()
	if item != nil {
		return item, true
	}

	b.Lock()
	defer b.Unlock()

	// check again under write lock
	item = b.lookup[key]
	if item != nil {
		return item, true
	}

	expires := time.Now().Add(duration).UnixNano()
	newItem := newItem(key, f(), expires, b.generation(), track)

	b.lookup[key] = newItem
	return newItem, false
}

func (b *bucket[T]) set(key string, value T, duration time.Duration, track bool) (*Item[T], *Item[T]) {
	expires := time.Now().Add(duration).UnixNano()
	item := newItem(key, value, expires, b.generation(), track)
	b.Lock()
	existing := b.lookup[key]
	b.lookup[key] = item
	b.Unlock()
	return item, existing
}

func (b *bucket[T]) generation() int64 {
	if b.nextGen == nil {
		return 0
	}
	return b.nextGen()
}

func (b *bucket[T]) remove(key string) *Item[T] {
	b.Lock()
	item := b.lookup[key]
	delete(b.lookup, key)
	b.Unlock()
	return item
}

// takeIfSame removes the key only if the mapped generation is still the one
// being evicted. It returns the detached item, or nil if a newer generation
// already replaced it. This keeps eviction from racing an overwrite.
func (b *bucket[T]) takeIfSame(key string, expected *Item[T]) *Item[T] {
	b.Lock()
	defer b.Unlock()
	if b.lookup[key] != expected {
		return nil
	}
	delete(b.lookup, key)
	return expected
}

// removeEntry detaches an item that was previously collected. It is a no-op
// when a newer generation occupies the key.
func (b *bucket[T]) removeEntry(item *Item[T]) bool {
	b.Lock()
	defer b.Unlock()
	if b.lookup[item.key] != item {
		return false
	}
	delete(b.lookup, item.key)
	return true
}

// removeEntryAt is removeEntry keyed by an explicit key, used by layered
// buckets.
func (b *bucket[T]) removeEntryAt(key string, expected *Item[T]) bool {
	b.Lock()
	defer b.Unlock()
	if b.lookup[key] != expected {
		return false
	}
	delete(b.lookup, key)
	return true
}

func (b *bucket[T]) delete(key string) {
	b.Lock()
	delete(b.lookup, key)
	b.Unlock()
}

// This is an expensive operation, so we do what we can to optimize it and limit
// the impact it has on concurrent operations. Specifically, we:
// 1 - Do an initial iteration to collect matches. This allows us to do the
//     "expensive" prefix check (on all values) using only a read-lock
// 2 - Do a second iteration, under write lock, for the matched results to do
//     the actual deletion

// The entries are handed to the caller (outside of every lock) for the
// asynchronous teardown: user supplied callbacks and event sinks must never
// run while a bucket lock is held.
func (b *bucket[T]) deleteFunc(matches func(key string, item *Item[T]) bool, deletables chan removal[T]) int {
	lookup := b.lookup
	items := make([]*Item[T], 0)

	b.Lock()
	for key, item := range lookup {
		if matches(key, item) {
			items = append(items, item)
		}
	}
	if len(items) == 0 {
		b.Unlock()
		return 0
	}
	for _, item := range items {
		if lookup[item.key] == item {
			delete(lookup, item.key)
		}
	}
	b.Unlock()

	for _, item := range items {
		deletables <- removal[T]{item: item, reason: ReasonExplicit}
	}
	return len(items)
}

func (b *bucket[T]) deletePrefix(prefix string, deletables chan removal[T]) int {
	return b.deleteFunc(func(key string, item *Item[T]) bool {
		return strings.HasPrefix(key, prefix)
	}, deletables)
}

// we expect the caller to have acquired a write lock
func (b *bucket[T]) clear() {
	for _, item := range b.lookup {
		item.promotions = -2
	}
	b.lookup = make(map[string]*Item[T])
}
