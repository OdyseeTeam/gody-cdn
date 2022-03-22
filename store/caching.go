package store

import (
	"crypto/sha1"
	"encoding/hex"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/reflector.go/shared"
	log "github.com/sirupsen/logrus"
)

// CachingStore combines two stores, typically a local and a remote store, to improve performance.
// Accessed objects are stored in and retrieved from the cache. If they are not in the cache, they
// are retrieved from the origin and cached. Puts are cached and also forwarded to the origin.
type CachingStore struct {
	origin, cache ObjectStore
	baseFuncs     *BaseFuncs
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

// NewCachingStoreV2 makes a new caching disk store that fetches object using a given function
func NewCachingStoreV2(component string, baseFuncs BaseFuncs, cache ObjectStore) *CachingStore {
	return &CachingStore{
		component: component,
		baseFuncs: &baseFuncs,
		cache:     WithSingleFlight(component, cache),
	}
}

const nameCaching = "caching"

// Name is the cache type name
func (c *CachingStore) Name() string { return nameCaching }

// Has checks the cache and then the origin for a hash. It returns true if either store has it.
func (c *CachingStore) Has(hash string, extra interface{}) (bool, error) {
	has, err := c.cache.Has(hash, extra)
	if has || err != nil {
		return has, err
	}
	if c.baseFuncs != nil {
		return c.baseFuncs.HasFunc(hash, extra)
	}
	return c.origin.Has(hash, extra)
}

// Get tries to get the object from the cache first, falling back to the origin. If the object comes
// from the origin, it is also stored in the cache.
// the extra parameter is used in conjunction with the getter function passed in V2 so that extra data such as decryption keys can be passed down
func (c *CachingStore) Get(originalName string, extra interface{}) ([]byte, shared.BlobTrace, error) {
	h := sha1.New()
	h.Write([]byte(originalName))
	hashedName := hex.EncodeToString(h.Sum(nil))
	start := time.Now()
	object, trace, err := c.cache.Get(hashedName, extra)
	if err == nil || !errors.Is(err, ErrObjectNotFound) {
		return object, trace.Stack(time.Since(start), c.Name()), err
	}
	if c.baseFuncs != nil {
		object, trace, err = c.baseFuncs.GetFunc(originalName, extra)
	} else {
		object, trace, err = c.origin.Get(originalName, extra)
	}
	if err != nil {
		return nil, trace.Stack(time.Since(start), c.Name()), err
	}
	// do not do this async unless you're prepared to deal with mayhem
	err = c.cache.Put(hashedName, object, extra)
	if err != nil {
		log.Errorf("error saving object to underlying cache: %s", errors.FullTrace(err))
	}
	return object, trace.Stack(time.Since(start), c.Name()), nil
}

// Put stores the object in the origin and the cache
func (c *CachingStore) Put(hash string, object []byte, extra interface{}) error {
	var err error
	if c.baseFuncs != nil {
		err = c.baseFuncs.PutFunc(hash, object, extra)
	} else {
		err = c.origin.Put(hash, object, extra)
	}
	if err != nil {
		return err
	}
	return c.cache.Put(hash, object, extra)
}

// Delete deletes the object from the origin and the cache
func (c *CachingStore) Delete(hash string, extra interface{}) error {
	var err error
	if c.baseFuncs != nil {
		err = c.baseFuncs.DelFunc(hash, extra)
	} else {
		err = c.origin.Delete(hash, extra)
	}
	if err != nil {
		return err
	}
	return c.cache.Delete(hash, extra)
}

// Shutdown shuts down the store gracefully
func (c *CachingStore) Shutdown() {
	if c.baseFuncs == nil {
		c.origin.Shutdown()
	}
	c.cache.Shutdown()
}
