package store

import (
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/reflector.go/shared"
)

// ObjectStore is an interface for handling object storage.
type ObjectStore interface {
	// Name of object store (useful for metrics)
	Name() string
	// Has Does object exist in the store.
	Has(hash string, extra interface{}) (bool, error)
	// Get the object from the store. Must return ErrObjectNotFound if object is not in store.
	Get(hash string, extra interface{}) ([]byte, shared.BlobTrace, error)
	// Put the object into the store.
	Put(hash string, object []byte, extra interface{}) error
	// Delete the object from the store.
	Delete(hash string, extra interface{}) error
	// Shutdown the store gracefully
	Shutdown()
}
type BaseFuncs struct {
	GetFunc func(hash string, extra interface{}) ([]byte, shared.BlobTrace, error)
	HasFunc func(hash string, extra interface{}) (bool, error)
	PutFunc func(hash string, object []byte, extra interface{}) error
	DelFunc func(hash string, extra interface{}) error
}

//ErrObjectNotFound is a standard error when an object is not found in the store.
var ErrObjectNotFound = errors.Base("object not found")
