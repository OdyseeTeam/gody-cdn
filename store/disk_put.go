//go:build darwin
// +build darwin

package store

import (
	"bytes"
	"io"
	"os"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

var openFileFlags = os.O_WRONLY | os.O_CREATE

// Put stores the object on disk
func (d *DiskStore) Put(hash string, object []byte, extra interface{}) error {
	err := d.ensureDirExists(d.dir(hash))
	if err != nil {
		return err
	}

	// Open file with O_DIRECT
	f, err := os.OpenFile(d.tmpPath(hash), openFileFlags, 0644)
	if err != nil {
		return errors.Err(err)
	}
	defer f.Close()

	_, err = io.Copy(f, bytes.NewReader(object))
	if err != nil {
		return errors.Err(err)
	}
	err = os.Rename(d.tmpPath(hash), d.path(hash))
	return errors.Err(err)
}
