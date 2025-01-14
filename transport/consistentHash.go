package transport

import (
	"github.com/spaolacci/murmur3"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint64

type ConsistentHash struct {
	hash     Hash
	replicas int
	keys     []uint64 // Sorted keys
	ring     map[uint64]string
}

// New creates a new ConsistentHash
func New(replicas int, fn Hash) *ConsistentHash {
	ch := &ConsistentHash{
		replicas: replicas,
		hash:     fn,
		ring:     make(map[uint64]string),
	}
	return ch
}

// Add adds a new consumer to the hash ring
func (ch *ConsistentHash) Add(consumers []string) {
	for _, consumer := range consumers {
		for i := 0; i < ch.replicas; i++ {
			key := ch.hash([]byte(consumer + "_" + strconv.Itoa(i)))
			ch.keys = append(ch.keys, key)
			ch.ring[key] = consumer
		}
	}
	sort.Slice(ch.keys, func(i, j int) bool {
		return ch.keys[i] < ch.keys[j]
	})
}

// Remove removes a consumer from the hash ring
func (ch *ConsistentHash) Remove(consumer string) {
	for i := 0; i < ch.replicas; i++ {
		key := ch.hash([]byte(consumer + "_" + strconv.Itoa(i)))
		delete(ch.ring, key)

		// Remove key from keys slice
		for j, k := range ch.keys {
			if k == key {
				ch.keys = append(ch.keys[:j], ch.keys[j+1:]...)
				break
			}
		}
	}
}

// Get gets the closest consumer for the given key
func (ch *ConsistentHash) Get(key string) string {
	if len(ch.keys) == 0 {
		return ""
	}

	hash := ch.hash([]byte(key))
	idx := sort.Search(len(ch.keys), func(i int) bool {
		return ch.keys[i] >= hash
	})

	return ch.ring[ch.keys[idx%len(ch.keys)]]
}

// MurmurHash function
func MurmurHash(data []byte) uint64 {
	return murmur3.Sum64(data)
}
