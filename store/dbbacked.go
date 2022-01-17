package store

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
)

// DBBackedStore is a store that's backed by a DB. The DB contains data about what's in the store.
type DBBackedStore struct {
	objectsStore ObjectStore
	conn         *sql.DB
}

// NewDBBackedStore returns an initialized store pointer.
func NewDBBackedStore(blobs ObjectStore, conn *sql.DB) *DBBackedStore {
	return &DBBackedStore{objectsStore: blobs, conn: conn}
}

const nameDBBacked = "db-backed"

// Name is the cache type name
func (d *DBBackedStore) Name() string { return nameDBBacked }

// Has returns true if the blob is in the store
func (d *DBBackedStore) Has(hash string) (bool, error) {
	stored, _, err := d.has(hash)
	return stored, err
}

// has returns true if the blob is in the store
func (d *DBBackedStore) has(hash string) (bool, *time.Time, error) {
	if d.conn == nil {
		return false, nil, errors.Err("not connected")
	}
	query := `SELECT hash, length, is_stored, last_accessed_at FROM object WHERE hash = ?`
	row := d.conn.QueryRow(query, hash)
	var queriedHash string
	var length uint
	var stored bool
	var lastAccess time.Time
	err := row.Scan(&queriedHash, &length, &stored, &lastAccess)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil, nil
		}
		return false, nil, errors.Err(err)
	}
	return stored, &lastAccess, nil
}

// Get gets the object
func (d *DBBackedStore) Get(hash string) ([]byte, shared.BlobTrace, error) {
	start := time.Now()
	has, lastAccess, err := d.has(hash)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(err)
	}
	if !has {
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), ErrObjectNotFound
	}

	obj, stack, err := d.objectsStore.Get(hash)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			e2 := d.Delete(hash)
			if e2 != nil {
				log.Errorf("error while deleting object from db: %s", errors.FullTrace(err))
			}
			return nil, stack.Stack(time.Since(start), d.Name()), ErrObjectNotFound
		}
	}
	if lastAccess.Before(time.Now().Add(-6 * time.Hour)) {
		err = d.touch(hash)
		if err != nil {
			log.Errorf("error while updating object's last access time on db: %s", errors.FullTrace(err))
		}
	}
	return obj, stack.Stack(time.Since(start), d.Name()), err
}

func (d *DBBackedStore) touch(hash string) error {
	if d.conn == nil {
		return false, nil, errors.Err("not connected")
	}
	query := `SELECT hash, length, is_stored, last_accessed_at FROM object WHERE hash = ?`
	row := d.conn.QueryRow(query, hash)
}

// Put stores the blob in the S3 store and stores the blob information in the DB.
func (d *DBBackedStore) Put(hash string, blob stream.Blob) error {
	err := d.blobs.Put(hash, blob)
	if err != nil {
		return err
	}

	return d.db.AddBlob(hash, len(blob), true)
}

// PutSD stores the SDBlob in the S3 store. It will return an error if the sd blob is missing the stream hash or if
// there is an error storing the blob information in the DB.
func (d *DBBackedStore) PutSD(hash string, blob stream.Blob) error {
	var blobContents db.SdBlob
	err := json.Unmarshal(blob, &blobContents)
	if err != nil {
		return errors.Err(err)
	}
	if blobContents.StreamHash == "" {
		return errors.Err("sd blob is missing stream hash")
	}

	err = d.blobs.PutSD(hash, blob)
	if err != nil {
		return err
	}

	return d.db.AddSDBlob(hash, len(blob), blobContents)
}

func (d *DBBackedStore) Delete(hash string) error {
	err := d.blobs.Delete(hash)
	if err != nil {
		return err
	}

	return d.db.Delete(hash)
}

// Block deletes the blob and prevents it from being uploaded in the future
func (d *DBBackedStore) Block(hash string) error {
	if blocked, err := d.isBlocked(hash); blocked || err != nil {
		return err
	}

	log.Debugf("blocking %s", hash)

	err := d.db.Block(hash)
	if err != nil {
		return err
	}

	//has, err := d.db.HasBlob(hash, false)
	//if err != nil {
	//	return err
	//}
	//
	//if has {
	//	err = d.blobs.Delete(hash)
	//	if err != nil {
	//		return err
	//	}
	//
	//	err = d.db.Delete(hash)
	//	if err != nil {
	//		return err
	//	}
	//}

	return d.markBlocked(hash)
}

// Wants returns false if the hash exists or is blocked, true otherwise
func (d *DBBackedStore) Wants(hash string) (bool, error) {
	blocked, err := d.isBlocked(hash)
	if blocked || err != nil {
		return false, err
	}

	has, err := d.Has(hash)
	return !has, err
}

// MissingBlobsForKnownStream returns missing blobs for an existing stream
// WARNING: if the stream does NOT exist, no blob hashes will be returned, which looks
// like no blobs are missing
func (d *DBBackedStore) MissingBlobsForKnownStream(sdHash string) ([]string, error) {
	return d.db.MissingBlobsForKnownStream(sdHash)
}

func (d *DBBackedStore) markBlocked(hash string) error {
	err := d.initBlocked()
	if err != nil {
		return err
	}

	d.blockedMu.Lock()
	defer d.blockedMu.Unlock()

	d.blocked[hash] = true
	return nil
}

func (d *DBBackedStore) isBlocked(hash string) (bool, error) {
	err := d.initBlocked()
	if err != nil {
		return false, err
	}

	d.blockedMu.RLock()
	defer d.blockedMu.RUnlock()

	return d.blocked[hash], nil
}

func (d *DBBackedStore) initBlocked() error {
	// first check without blocking since this is the most likely scenario
	if d.blocked != nil {
		return nil
	}

	d.blockedMu.Lock()
	defer d.blockedMu.Unlock()

	// check again in case of race condition
	if d.blocked != nil {
		return nil
	}

	var err error
	d.blocked, err = d.db.GetBlocked()

	return err
}

// Shutdown shuts down the store gracefully
func (d *DBBackedStore) Shutdown() {
	d.blobs.Shutdown()
}
