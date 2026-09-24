// An LRU cached aimed at high concurrency
package ccache

import (
	"sync/atomic"
	"time"
)

type LayeredCache[T any] struct {
	*Configuration[T]
	control
	list            *List[T]
	buckets         []*layeredBucket[T]
	bucketMask      uint32
	size            int64
	pruneTargetSize int64
	deletables      chan removal[T]
	promotables     chan *Item[T]
	events          *eventHub
	generations     generationCounter
}

// Create a new layered cache with the specified configuration.
// A layered cache used a two keys to identify a value: a primary key
// and a secondary key. Get, Set and Delete require both a primary and a
// secondary key. However, DeleteAll requires only a primary key, deleting
// all values that share the same primary key.

// Layered Cache is useful as an HTTP cache, where an HTTP purge might
// delete multiple variants of the same resource:
// primary key = "user/44"
// secondary key 1 = ".json"
// secondary key 2 = ".xml"

// See ccache.Configure() for creating a configuration
func Layered[T any](config *Configuration[T]) *LayeredCache[T] {
	c := &LayeredCache[T]{
		list:            NewList[T](),
		Configuration:   config,
		control:         newControl(),
		bucketMask:      uint32(config.buckets) - 1,
		buckets:         make([]*layeredBucket[T], config.buckets),
		deletables:      make(chan removal[T], config.deleteBuffer),
		promotables:     make(chan *Item[T], config.promoteBuffer),
		pruneTargetSize: config.maxSize - config.maxSize*int64(config.percentToPrune)/100,
		events:          newEventHub(config.sink, config.eventBuffer),
	}
	for i := 0; i < config.buckets; i++ {
		c.buckets[i] = &layeredBucket[T]{
			buckets: make(map[string]*bucket[T]),
			nextGen: c.generations.generation,
		}
	}
	go c.worker()
	return c
}

func (c *LayeredCache[T]) ItemCount() int {
	count := 0
	for _, b := range c.buckets {
		count += b.itemCount()
	}
	return count
}

// Get an item from the cache. Returns nil if the item wasn't found.
// This can return an expired item. Use item.Expired() to see if the item
// is expired and item.TTL() to see how long until the item expires (which
// will be negative for an already expired item).
func (c *LayeredCache[T]) Get(primary, secondary string) *Item[T] {
	bucket := c.bucket(primary)
	item := bucket.get(primary, secondary)
	if item == nil {
		return nil
	}
	if item.Expired() {
		c.expire(bucket, primary, secondary, item)
		return item
	}
	c.queuePromotion(item, false)
	return item
}

// Same as Get but does not promote the value. This essentially circumvents the
// "least recently used" aspect of this cache. To some degree, it's akin to a
// "peak"
func (c *LayeredCache[T]) GetWithoutPromote(primary, secondary string) *Item[T] {
	return c.bucket(primary).get(primary, secondary)
}

func (c *LayeredCache[T]) ForEachFunc(primary string, matches func(key string, item *Item[T]) bool) {
	c.bucket(primary).forEachFunc(primary, matches)
}

// Get the secondary cache for a given primary key. This operation will
// never return nil. In the case where the primary key does not exist, a
// new, underlying, empty bucket will be created and returned.
func (c *LayeredCache[T]) GetOrCreateSecondaryCache(primary string) *SecondaryCache[T] {
	primaryBkt := c.bucket(primary)
	bkt := primaryBkt.getSecondaryBucket(primary)
	primaryBkt.Lock()
	if bkt == nil {
		bkt = &bucket[T]{lookup: make(map[string]*Item[T]), nextGen: c.generations.generation}
		primaryBkt.buckets[primary] = bkt
	}
	primaryBkt.Unlock()
	return &SecondaryCache[T]{
		bucket: bkt,
		pCache: c,
	}
}

// Used when the cache was created with the Track() configuration option.
// Avoid otherwise
func (c *LayeredCache[T]) TrackingGet(primary, secondary string) TrackedItem[T] {
	item := c.Get(primary, secondary)
	if item == nil {
		return nil
	}
	item.track()
	return item
}

// Set the value in the cache for the specified duration
func (c *LayeredCache[T]) TrackingSet(primary, secondary string, value T, duration time.Duration) TrackedItem[T] {
	return c.set(primary, secondary, value, duration, true)
}

// Set the value in the cache for the specified duration
func (c *LayeredCache[T]) Set(primary, secondary string, value T, duration time.Duration) {
	c.set(primary, secondary, value, duration, false)
}

// Replace the value if it exists, does not set if it doesn't.
// Returns true if the item existed an was replaced, false otherwise.
// Replace does not reset item's TTL nor does it alter its position in the LRU
func (c *LayeredCache[T]) Replace(primary, secondary string, value T) bool {
	item := c.bucket(primary).get(primary, secondary)
	if item == nil {
		return false
	}
	c.Set(primary, secondary, value, item.TTL())
	return true
}

// Attempts to get the value from the cache and calles fetch on a miss.
// If fetch returns an error, no value is cached and the error is returned back
// to the caller.
// Note that Fetch merely calls the public Get and Set functions. If you want
// a different Fetch behavior, such as thundering herd protection or returning
// expired items, implement it in your application.
func (c *LayeredCache[T]) Fetch(primary, secondary string, duration time.Duration, fetch func() (T, error)) (*Item[T], error) {
	item := c.Get(primary, secondary)
	if item != nil {
		return item, nil
	}
	value, err := fetch()
	if err != nil {
		return nil, err
	}
	return c.set(primary, secondary, value, duration, false), nil
}

// Remove the item from the cache, return true if the item was present, false otherwise.
func (c *LayeredCache[T]) Delete(primary, secondary string) bool {
	item := c.bucket(primary).remove(primary, secondary)
	if item != nil {
		c.enqueueRemoval(item, ReasonExplicit)
		return true
	}
	return false
}

// Deletes all items that share the same primary key
func (c *LayeredCache[T]) DeleteAll(primary string) bool {
	items, ok := c.bucket(primary).detachAll(primary)
	if !ok {
		return false
	}
	for _, item := range items {
		c.enqueueRemoval(item, ReasonExplicit)
	}
	return true
}

// Deletes all items that share the same primary key and prefix.
func (c *LayeredCache[T]) DeletePrefix(primary, prefix string) int {
	return c.bucket(primary).deletePrefix(primary, prefix, c.deletables)
}

// Deletes all items that share the same primary key and where the matches func evaluates to true.
func (c *LayeredCache[T]) DeleteFunc(primary string, matches func(key string, item *Item[T]) bool) int {
	return c.bucket(primary).deleteFunc(primary, matches, c.deletables)
}

func (c *LayeredCache[T]) set(primary, secondary string, value T, duration time.Duration, track bool) *Item[T] {
	item, existing := c.bucket(primary).set(primary, secondary, value, duration, track)
	if existing != nil {
		c.enqueueRemoval(existing, ReasonReplaced)
		c.installed(item, ReasonOverwritten)
	} else {
		c.installed(item, ReasonInsert)
	}
	c.promote(item)
	return item
}

func (c *LayeredCache[T]) installed(item *Item[T], reason Reason) {
	c.events.emit(c.event(EventSet, reason, item))
}

func (c *LayeredCache[T]) event(typ EventType, reason Reason, item *Item[T]) Event {
	return Event{
		Type:       typ,
		Reason:     reason,
		Key:        item.key,
		Group:      item.group,
		Generation: item.Generation(),
		Size:       item.size,
		At:         time.Now(),
	}
}

func (c *LayeredCache[T]) enqueueRemoval(item *Item[T], reason Reason) {
	c.deletables <- removal[T]{item: item, reason: reason}
}

func (c *LayeredCache[T]) expire(bucket *layeredBucket[T], primary, secondary string, item *Item[T]) {
	if c.events == nil {
		return
	}
	select {
	case c.deletables <- removal[T]{item: item, reason: ReasonTTL}:
		bucket.removeEntry(primary, secondary, item)
	default:
	}
}

func (c *LayeredCache[T]) bucket(key string) *layeredBucket[T] {
	return c.buckets[fnv1a32(key)&c.bucketMask]
}

func (c *LayeredCache[T]) halted(fn func()) {
	c.halt()
	defer c.unhalt()
	fn()
}

func (c *LayeredCache[T]) halt() {
	for _, bucket := range c.buckets {
		bucket.Lock()
	}
}

func (c *LayeredCache[T]) unhalt() {
	for _, bucket := range c.buckets {
		bucket.Unlock()
	}
}

func (c *LayeredCache[T]) promote(item *Item[T]) {
	c.promotables <- item
}

func (c *LayeredCache[T]) queuePromotion(item *Item[T], initial bool) {
	if initial {
		c.promotables <- item
		return
	}
	select {
	case c.promotables <- item:
	default:
		c.events.emit(c.event(EventPromotion, ReasonQueueFull, item))
	}
}

// GetDroppedEvents returns the number of lifecycle events that were discarded
// because the configured event sink could not keep up. The counter is
// cumulative and never reset.
func (c *LayeredCache[T]) GetDroppedEvents() int64 {
	return c.events.droppedCount()
}

// Stop shuts down the worker and the event delivery goroutine.
func (c *LayeredCache[T]) Stop() {
	c.control.Stop()
	c.events.close()
}

func (c *LayeredCache[T]) worker() {
	dropped := 0
	cc := c.control

	promoteItem := func(item *Item[T]) {
		if c.doPromote(item) && c.size > c.maxSize {
			dropped += c.gc()
		}
	}

	for {
		select {
		case item := <-c.promotables:
			promoteItem(item)
		case removal := <-c.deletables:
			c.doDelete(removal.item, removal.reason)
		case control := <-cc:
			switch msg := control.(type) {
			case controlStop:
				goto drain
			case controlGetDropped:
				msg.res <- dropped
				dropped = 0
			case controlSetMaxSize:
				newMaxSize := msg.size
				c.maxSize = newMaxSize
				c.pruneTargetSize = newMaxSize - newMaxSize*int64(c.percentToPrune)/100
				if c.size > c.maxSize {
					dropped += c.gc()
				}
				msg.done <- struct{}{}
			case controlClear:
				promotables := c.promotables
				for len(promotables) > 0 {
					<-promotables
				}
				deletables := c.deletables
				for len(deletables) > 0 {
					<-deletables
				}

				c.halted(func() {
					for _, bucket := range c.buckets {
						bucket.clear()
					}
					c.size = 0
					c.list = NewList[T]()
				})
				msg.done <- struct{}{}
			case controlGetSize:
				msg.res <- c.size
			case controlGC:
				dropped += c.gc()
				msg.done <- struct{}{}
			case controlSyncUpdates:
				doAllPendingPromotesAndDeletes(c.promotables, promoteItem, c.deletables, c.doDelete)
				msg.done <- struct{}{}
			}
		}
	}

drain:
	for {
		select {
		case removal := <-c.deletables:
			c.doDelete(removal.item, removal.reason)
		default:
			return
		}
	}
}

func (c *LayeredCache[T]) doDelete(item *Item[T], reason Reason) {
	if !item.inList {
		if item.markTerminal() {
			c.emitTerminal(item, reason)
		}
		return
	}
	c.size -= item.size
	if c.onDelete != nil {
		c.onDelete(item)
	}
	c.list.Remove(item)
	if item.markTerminal() {
		c.emitTerminal(item, reason)
	}
}

func (c *LayeredCache[T]) emitTerminal(item *Item[T], reason Reason) {
	var typ EventType
	switch reason {
	case ReasonTTL:
		typ = EventExpiration
	case ReasonExplicit:
		typ = EventDelete
	case ReasonReplaced:
		typ = EventReplace
	default:
		typ = EventDelete
	}
	c.events.emit(c.event(typ, reason, item))
}

func (c *LayeredCache[T]) doPromote(item *Item[T]) bool {
	// deleted before it ever got promoted
	if item.terminal() {
		return false
	}

	if item.inList {
		if item.shouldPromote(c.getsPerPromote) {
			c.list.MoveToFront(item)
			atomic.StoreInt32(&item.promotions, 0)
			c.events.emit(c.event(EventPromotion, ReasonMoved, item))
		}
		return false
	}

	c.size += item.size
	c.list.Insert(item)
	c.events.emit(c.event(EventPromotion, ReasonQueued, item))
	return true
}

func (c *LayeredCache[T]) gc() int {
	dropped := 0
	item := c.list.Tail

	prunedSize := int64(0)
	sizeToPrune := c.size - c.pruneTargetSize

	for prunedSize < sizeToPrune {
		if item == nil {
			return dropped
		}
		prev := item.prev
		if !c.tracking || atomic.LoadInt32(&item.refCount) == 0 {
			if detached := c.bucket(item.group).takeIfSame(item.group, item.key, item); detached != nil && item.markTerminal() {
				itemSize := item.size
				c.size -= itemSize
				prunedSize += itemSize

				c.list.Remove(item)
				if c.onDelete != nil {
					c.onDelete(item)
				}
				c.events.emit(c.event(EventEviction, ReasonCapacity, item))
				dropped += 1
			}
		}
		item = prev
	}
	return dropped
}
