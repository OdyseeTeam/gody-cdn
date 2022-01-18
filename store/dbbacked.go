package store

import (
	"database/sql"
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	qt "github.com/lbryio/lbry.go/v2/extras/query"
	log "github.com/sirupsen/logrus"
)

// DBBackedStore is a store that's backed by a DB. The DB contains data about what's in the store.
type DBBackedStore struct {
	objectsStore ObjectStore
	conn         *sql.DB
	ticker       *time.Ticker
	done         chan bool
}

// NewDBBackedStore returns an initialized store pointer.
func NewDBBackedStore(blobs ObjectStore, conn *sql.DB) *DBBackedStore {
	return &DBBackedStore{objectsStore: blobs, conn: conn, ticker: time.NewTicker(5 * time.Minute), done: make(chan bool)}
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
		return errors.Err("not connected")
	}
	query := `UPDATE object set last_accessed_at = ? WHERE hash = ?`
	_, err := d.conn.Exec(query, time.Now(), hash)
	return errors.Err(err)
}

// Put stores the blob in the S3 store and stores the blob information in the DB.
func (d *DBBackedStore) Put(hash string, object []byte) error {
	if d.conn == nil {
		return errors.Err("not connected")
	}
	err := d.objectsStore.Put(hash, object)
	if err != nil {
		return err
	}
	args := []interface{}{hash, true, len(object), time.Now()}
	query := `INSERT INTO object (hash,is_stored,length,last_accessed_at) VALUES(` + qt.Qs(len(args)) + `) ON DUPLICATE KEY UPDATE is_stored = (is_stored or VALUES(is_stored)), last_accessed_at = VALUES(last_accessed_at)`
	_, err = d.conn.Exec(query, args...)
	return errors.Err(err)
}

func (d *DBBackedStore) Delete(hash string) error {
	if d.conn == nil {
		return errors.Err("not connected")
	}
	err := d.objectsStore.Delete(hash)
	if err != nil {
		return err
	}
	query := `DELETE FROM object WHERE hash = ?`
	_, err = d.conn.Exec(query, hash)
	return errors.Err(err)
}

// Shutdown shuts down the store gracefully
func (d *DBBackedStore) Shutdown() {
	d.ticker.Stop()
	d.done <- true
	d.objectsStore.Shutdown()
}

func (d *DBBackedStore) selfCleanup() {
	alreadyRunning := false
	for {
		select {
		case <-d.done:
			return
		case t := <-d.ticker.C:
			if alreadyRunning {
				log.Infoln("Skipping self cleanup as it's already running")
			}
			alreadyRunning = true
			log.Infoln("Beginning self cleanup...")
			//select objects to delete and delete them
			log.Infof("Finished self cleanup. It took %s", time.Since(t).String())
		}
	}

}
