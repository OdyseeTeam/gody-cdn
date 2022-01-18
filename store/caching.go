package store

import (
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	log "github.com/sirupsen/logrus"
)

// CachingStore combines two stores, typically a local and a remote store, to improve performance.
// Accessed objects are stored in and retrieved from the cache. If they are not in the cache, they
// are retrieved from the origin and cached. Puts are cached and also forwarded to the origin.
type CachingStore struct {
	origin, cache ObjectStore
	component     string
}

// NewCachingStore makes a new caching disk store and returns a pointer to it.
func NewCachingStore(component string, origin, cache ObjectStore) *CachingStore {
	return &CachingStore{
		component: component,
		origin:    WithSingleFlight(component, origin),
		cache:     WithSingleFlight(component, cache),
	}
}

const nameCaching = "caching"

// Name is the cache type name
func (c *CachingStore) Name() string { return nameCaching }

// Has checks the cache and then the origin for a hash. It returns true if either store has it.
func (c *CachingStore) Has(hash string) (bool, error) {
	has, err := c.cache.Has(hash)
	if has || err != nil {
		return has, err
	}
	return c.origin.Has(hash)
}

// Get tries to get the object from the cache first, falling back to the origin. If the object comes
// from the origin, it is also stored in the cache.
func (c *CachingStore) Get(hash string) ([]byte, shared.BlobTrace, error) {
	start := time.Now()
	object, trace, err := c.cache.Get(hash)
	if err == nil || !errors.Is(err, ErrObjectNotFound) {
		return object, trace.Stack(time.Since(start), c.Name()), err
	}

	object, trace, err = c.origin.Get(hash)
	if err != nil {
		return nil, trace.Stack(time.Since(start), c.Name()), err
	}
	// do not do this async unless you're prepared to deal with mayhem
	err = c.cache.Put(hash, object)
	if err != nil {
		log.Errorf("error saving object to underlying cache: %s", errors.FullTrace(err))
	}
	return object, trace.Stack(time.Since(start), c.Name()), nil
}

// Put stores the object in the origin and the cache
func (c *CachingStore) Put(hash string, object []byte) error {
	err := c.origin.Put(hash, object)
	if err != nil {
		return err
	}
	return c.cache.Put(hash, object)
}

// Delete deletes the object from the origin and the cache
func (c *CachingStore) Delete(hash string) error {
	err := c.origin.Delete(hash)
	if err != nil {
		return err
	}
	return c.cache.Delete(hash)
}

// Shutdown shuts down the store gracefully
func (c *CachingStore) Shutdown() {
	c.origin.Shutdown()
	c.cache.Shutdown()
}
