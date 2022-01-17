package store

import (
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

// ObjectStore is an interface for handling blob storage.
type ObjectStore interface {
	// Name of blob store (useful for metrics)
	Name() string
	// Has Does blob exist in the store.
	Has(hash string) (bool, error)
	// Get the blob from the store. Must return ErrObjectNotFound if blob is not in store.
	Get(hash string) ([]byte, shared.BlobTrace, error)
	// Put the blob into the store.
	Put(hash string, object []byte) error
	// Delete the blob from the store.
	Delete(hash string) error
	// Shutdown the store gracefully
	Shutdown()
}

//ErrObjectNotFound is a standard error when an object is not found in the store.
var ErrObjectNotFound = errors.Base("object not found")
