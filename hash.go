package ccache

// fnv1a32 computes the FNV-1a 32 bit hash of key. It is inlined by design:
// allocating a hash.Hash32 on every Get would put a heap allocation on the
// hottest read path.
func fnv1a32(key string) uint32 {
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	hash := offset32
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}
