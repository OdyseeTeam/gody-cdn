package store

import (
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

// ObjectStore is an interface for handling object storage.
type ObjectStore interface {
	// Name of object store (useful for metrics)
	Name() string
	// Has Does object exist in the store.
	Has(hash string) (bool, error)
	// Get the object from the store. Must return ErrObjectNotFound if object is not in store.
	Get(hash string) ([]byte, shared.BlobTrace, error)
	// Put the object into the store.
	Put(hash string, object []byte) error
	// Delete the object from the store.
	Delete(hash string) error
	// Shutdown the store gracefully
	Shutdown()
}

//ErrObjectNotFound is a standard error when an object is not found in the store.
var ErrObjectNotFound = errors.Base("object not found")
