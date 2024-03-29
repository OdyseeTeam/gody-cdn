package store

import (
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/lbryio/reflector.go/shared"
	"github.com/lbryio/reflector.go/store/speedwalk"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

// DiskStore stores objects on a local disk
type DiskStore struct {
	// the location of objects on disk
	objectDir string
	// store files in subdirectories based on the first N chars in the filename. 0 = don't create subdirectories.
	prefixLength int

	// true if initOnce ran, false otherwise
	initialized bool
}

// NewDiskStore returns an initialized file disk store pointer.
func NewDiskStore(dir string, prefixLength int) (*DiskStore, error) {
	ds := &DiskStore{
		objectDir:    dir,
		prefixLength: prefixLength,
	}
	err := ds.initOnce()
	return ds, err
}

const nameDisk = "disk"

// Name is the cache type name
func (d *DiskStore) Name() string { return nameDisk }

// Has returns whether the object exists or not. It will error with any IO disk error.
func (d *DiskStore) Has(hash string, extra interface{}) (bool, error) {
	_, err := os.Stat(d.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Err(err)
	}
	return true, nil
}

// Get returns the object or an error if the object doesn't exist.
func (d *DiskStore) Get(hash string, extra interface{}) ([]byte, shared.BlobTrace, error) {
	start := time.Now()

	object, err := ioutil.ReadFile(d.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(ErrObjectNotFound)
		}
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(err)
	}
	return object, shared.NewBlobTrace(time.Since(start), d.Name()), nil
}

// Delete deletes the object from the store
func (d *DiskStore) Delete(hash string, extra interface{}) error {
	err := os.Remove(d.path(hash))
	if os.IsNotExist(err) {
		return nil
	}
	return errors.Err(err)
}

// list returns the hashes of objects that already exist in the objectDir
func (d *DiskStore) list() ([]string, error) {
	return speedwalk.AllFiles(d.objectDir, true)
}

func (d *DiskStore) dir(hash string) string {
	if d.prefixLength <= 0 || len(hash) < d.prefixLength {
		return d.objectDir
	}
	return path.Join(d.objectDir, hash[:d.prefixLength])
}
func (d *DiskStore) tmpDir(hash string) string {
	return path.Join(d.objectDir, "tmp")
}
func (d *DiskStore) path(hash string) string {
	return path.Join(d.dir(hash), hash)
}
func (d *DiskStore) tmpPath(hash string) string {
	return path.Join(d.tmpDir(hash), hash)
}
func (d *DiskStore) ensureDirExists(dir string) error {
	return errors.Err(os.MkdirAll(dir, 0755))
}

func (d *DiskStore) initOnce() error {
	if d.initialized {
		return nil
	}

	err := d.ensureDirExists(d.objectDir)
	if err != nil {
		return err
	}
	err = d.ensureDirExists(path.Join(d.objectDir, "tmp"))
	if err != nil {
		return err
	}
	d.initialized = true
	return nil
}

// Shutdown shuts down the store gracefully
func (d *DiskStore) Shutdown() {
}
