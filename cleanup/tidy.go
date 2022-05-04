package cleanup

import (
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/OdyseeTeam/gody-cdn/configs"
	"github.com/OdyseeTeam/gody-cdn/store"
	
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"
	"github.com/sirupsen/logrus"
)

func SelfCleanup(dbStore *store.DBBackedStore, outerStore store.ObjectStore, stopper *stop.Group, diskConfig configs.ObjectCacheParams) {
	err := doClean(dbStore, outerStore, stopper, diskConfig)
	if err != nil {
		logrus.Error(errors.FullTrace(err))
	}
	const cleanupInterval = 2 * time.Minute
	for {
		select {
		case <-stopper.Ch():
			logrus.Infoln("stopping self cleanup")
			return
		case <-time.After(cleanupInterval):
			err := doClean(dbStore, outerStore, stopper, diskConfig)
			if err != nil {
				logrus.Error(errors.FullTrace(err))
			}
		}
	}
}

func doClean(dbStore *store.DBBackedStore, outerStore store.ObjectStore, stopper *stop.Group, diskConfig configs.ObjectCacheParams) error {
	used, err := GetUsedSpace(dbStore, diskConfig.Path)
	if err != nil {
		return err
	}
	if used >= diskConfig.GetMaxSize() {
		startTime := time.Now()
		pruneAmount := used - diskConfig.GetMaxSize() + int(float64(used)/100.*5)
		objectsToDelete, err := dbStore.LeastRecentlyAccessedObjects(pruneAmount)
		logrus.Infof("[godycdn] cleanup triggered. Used: %dG, maxsize: %dG, pruneamount: %dG", used/1024/1024/1024, diskConfig.GetMaxSize()/1024/1024/1024, pruneAmount/1024/1024/1024)
		if err != nil {
			return err
		}
		objectsChan := make(chan string, len(objectsToDelete))
		wg := &stop.Group{}
		go func() {
			for _, hash := range objectsToDelete {
				select {
				case <-stopper.Ch():
					return
				default:
				}
				objectsChan <- hash
			}
			close(objectsChan)
		}()
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for h := range objectsChan {
					select {
					case <-stopper.Ch():
						return
					default:
					}
					err = outerStore.Delete(h, nil)
					if err != nil {
						logrus.Errorf("error pruning %s: %s", h, errors.FullTrace(err))
						continue
					}
				}
			}()
		}
		wg.Wait()
		logrus.Infof("[godycdn] cleanup finished - it took %s", time.Since(startTime))
	}
	return nil
}

// GetUsedSpace returns how many bytes are used in the partition hosting the path
// setting SPACE_USE_DB=true as env var will force the function to calculate stored size from db info
func GetUsedSpace(dbStore *store.DBBackedStore, path string) (int, error) {
	useDB := os.Getenv("SPACE_USE_DB")
	queryDb := false
	if useDB != "" {
		b, err := strconv.ParseBool(useDB)
		if err == nil {
			queryDb = b
		}
	}
	if queryDb {
		return dbStore.UsedSpace(true)
	}
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})

	return int(size), errors.Err(err)
}
