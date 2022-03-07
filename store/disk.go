package store

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/lbryio/reflector.go/shared"
	"github.com/lbryio/reflector.go/store/speedwalk"

	"github.com/brk0v/directio"
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
func NewDiskStore(dir string, prefixLength int) *DiskStore {
	return &DiskStore{
		objectDir:    dir,
		prefixLength: prefixLength,
	}
}

const nameDisk = "disk"

// Name is the cache type name
func (d *DiskStore) Name() string { return nameDisk }

// Has returns whether the object exists or not. It will error with any IO disk error.
func (d *DiskStore) Has(hash string) (bool, error) {
	err := d.initOnce()
	if err != nil {
		return false, err
	}

	_, err = os.Stat(d.path(hash))
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
	err := d.initOnce()
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), err
	}

	object, err := ioutil.ReadFile(d.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(ErrObjectNotFound)
		}
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(err)
	}
	return object, shared.NewBlobTrace(time.Since(start), d.Name()), nil
}

// Put stores the object on disk
func (d *DiskStore) Put(hash string, object []byte) error {
	err := d.initOnce()
	if err != nil {
		return err
	}

	err = d.ensureDirExists(d.dir(hash))
	if err != nil {
		return err
	}

	// Open file with O_DIRECT
	f, err := os.OpenFile(d.tmpPath(hash), openFileFlags, 0644)
	if err != nil {
		return errors.Err(err)
	}
	defer f.Close()

	// Use directio writer
	dio, err := directio.New(f)
	if err != nil {
		return errors.Err(err)
	}
	defer dio.Flush()
	// Write the body to file
	_, err = io.Copy(dio, bytes.NewReader(object))
	if err != nil {
		return errors.Err(err)
	}
	err = os.Rename(d.tmpPath(hash), d.path(hash))
	return errors.Err(err)
}

// Delete deletes the object from the store
func (d *DiskStore) Delete(hash string) error {
	err := d.initOnce()
	if err != nil {
		return err
	}

	has, err := d.Has(hash)
	if err != nil {
		return err
	}
	if !has {
		return nil
	}

	err = os.Remove(d.path(hash))
	return errors.Err(err)
}

// list returns the hashes of objects that already exist in the objectDir
func (d *DiskStore) list() ([]string, error) {
	err := d.initOnce()
	if err != nil {
		return nil, err
	}

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
