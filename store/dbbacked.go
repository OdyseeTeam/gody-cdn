package store

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	qt "github.com/lbryio/lbry.go/v2/extras/query"
	"github.com/lbryio/reflector.go/shared"
	log "github.com/sirupsen/logrus"
)

/*
Table description:
CREATE TABLE `object`
(
    `id`               bigint unsigned                  NOT NULL AUTO_INCREMENT,
    `hash`             char(64) COLLATE utf8_unicode_ci NOT NULL,
    `is_stored`        tinyint(1)                       NOT NULL DEFAULT '0',
    `length`           bigint unsigned                           DEFAULT NULL,
    `last_accessed_at` timestamp                        NULL     DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `id` (`id`),
    UNIQUE KEY `hash_idx` (`hash`),
    KEY `last_accessed_idx` (`last_accessed_at`),
    KEY `is_stored_idx` (`is_stored`)
);
*/

// DBBackedStore is a store that's backed by a DB. The DB contains data about what's in the store.
type DBBackedStore struct {
	objectsStore ObjectStore
	conn         *sql.DB
	done         chan bool
}

// NewDBBackedStore returns an initialized store pointer.
func NewDBBackedStore(objectStore ObjectStore, dsn string) *DBBackedStore {
	conn, err := connect(dsn)
	if err != nil {
		log.Fatalln(errors.FullTrace(err))
	}
	return &DBBackedStore{objectsStore: objectStore, conn: conn, done: make(chan bool)}
}

// Connect will create a connection to the database
func connect(dsn string) (*sql.DB, error) {
	var err error
	dsn += "?parseTime=1&collation=utf8mb4_unicode_ci"
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Err(err)
	}

	conn.SetMaxIdleConns(12)

	return conn, errors.Err(conn.Ping())
}

const nameDBBacked = "db-backed"

// Name is the cache type name
func (d *DBBackedStore) Name() string { return nameDBBacked }

// UsedSpace returns how many bytes are currently indexed by the db store
// is_stored is always 1 in the current implementation, so avoiding that filter by passing fast=true the query return faster without any extra indexes
func (d *DBBackedStore) UsedSpace(fast bool) (int, error) {
	if d.conn == nil {
		return 0, errors.Err("not connected")
	}
	query := `select sum(length) AS total FROM object WHERE is_stored = 1`
	if fast {
		query = `select sum(length) AS total FROM object`
	}
	row := d.conn.QueryRow(query)
	var total int
	err := row.Scan(&total)
	if err != nil {
		return 0, errors.Err(err)
	}
	return total, nil
}

// Has returns true if the object is in the store
func (d *DBBackedStore) Has(hash string, extra interface{}) (bool, error) {
	stored, _, err := d.has(hash)
	return stored, err
}

// has returns true if the object is in the store
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
func (d *DBBackedStore) Get(hash string, extra interface{}) ([]byte, shared.BlobTrace, error) {
	start := time.Now()
	has, lastAccess, err := d.has(hash)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(err)
	}
	if !has {
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), ErrObjectNotFound
	}

	obj, stack, err := d.objectsStore.Get(hash, extra)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			e2 := d.Delete(hash, extra)
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

// Put stores the object in the S3 store and stores the object information in the DB.
func (d *DBBackedStore) Put(hash string, object []byte, extra interface{}) error {
	if d.conn == nil {
		return errors.Err("not connected")
	}
	err := d.objectsStore.Put(hash, object, extra)
	if err != nil {
		return err
	}
	args := []interface{}{hash, true, len(object), time.Now()}
	query := `INSERT INTO object (hash,is_stored,length,last_accessed_at) VALUES(` + qt.Qs(len(args)) + `) ON DUPLICATE KEY UPDATE is_stored = (is_stored or VALUES(is_stored)), last_accessed_at = VALUES(last_accessed_at)`
	_, err = d.conn.Exec(query, args...)
	return errors.Err(err)
}

func (d *DBBackedStore) Delete(hash string, extra interface{}) error {
	if d.conn == nil {
		return errors.Err("not connected")
	}
	err := d.objectsStore.Delete(hash, extra)
	if err != nil {
		return err
	}
	query := `DELETE FROM object WHERE hash = ?`
	_, err = d.conn.Exec(query, hash)
	return errors.Err(err)
}

// Shutdown shuts down the store gracefully
func (d *DBBackedStore) Shutdown() {
	d.done <- true
	d.objectsStore.Shutdown()
}

// LeastRecentlyAccessedObjects retrieves as many objects from the database as needed to match totalSize in occupied bytes
func (d *DBBackedStore) LeastRecentlyAccessedObjects(totalSize int) ([]string, error) {
	if d.conn == nil {
		return nil, errors.Err("not connected")
	}
	retrievedSize := 0
	hashes := make([]string, 0, 1000)
	for i := 0; retrievedSize < totalSize; i++ {
		objects, err := d.leastRecentlyAccessedObjects(i)
		if err != nil {
			return nil, err
		}
		if len(objects) == 0 {
			return hashes, nil
		}
		for _, o := range objects {
			retrievedSize += o.size
			hashes = append(hashes, o.hash)
			if retrievedSize >= totalSize {
				return hashes, nil
			}
		}
	}
	return hashes, nil
}

type dbObject struct {
	hash string
	size int
}

//leastRecentlyAccessedObjects retrieves objects in chunks at a time starting from lastOffset
func (d *DBBackedStore) leastRecentlyAccessedObjects(lastOffset int) ([]dbObject, error) {
	limit := 50000
	fast := true

	query := "SELECT hash, length from object where is_stored = 1 order by last_accessed_at limit ? offset ?"
	if fast {
		query = "SELECT hash, length from object order by last_accessed_at limit ? offset ?"
	}

	rows, err := d.conn.Query(query, limit, lastOffset*limit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.Err(err)
	}
	defer rows.Close()

	objects := make([]dbObject, 0, limit)
	for rows.Next() {
		var hash string
		var size int
		err := rows.Scan(&hash, &size)
		if err != nil {
			return nil, errors.Err(err)
		}
		objects = append(objects, dbObject{
			hash: hash,
			size: size,
		})
	}
	return objects, nil
}
