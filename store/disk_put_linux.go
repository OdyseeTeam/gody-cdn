package store

import (
	"bytes"
	"io"
	"os"
	"syscall"

	"github.com/brk0v/directio"
	"github.com/lbryio/lbry.go/v2/extras/errors"
)

var openFileFlags = os.O_WRONLY | os.O_CREATE | syscall.O_DIRECT

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
