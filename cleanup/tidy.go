package cleanup

import (
	"syscall"
	"time"

	"github.com/OdyseeTeam/gody-cdn/configs"
	"github.com/OdyseeTeam/gody-cdn/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"
	"github.com/sirupsen/logrus"
)

func SelfCleanup(dbStore *store.DBBackedStore, outerStore store.ObjectStore, stopper *stop.Group, diskConfig configs.ObjectCacheParams) {
	// this is so that it runs on startup without having to wait for 10 minutes
	err := doClean(dbStore, outerStore, stopper, diskConfig)
	if err != nil {
		logrus.Error(errors.FullTrace(err))
	}
	const cleanupInterval = 10 * time.Minute
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
	used, err := GetUsedSpace(diskConfig.Path)
	if err != nil {
		return err
	}
	if used >= diskConfig.GetMaxSize() {
		objectsToDelete, err := dbStore.LeastRecentlyAccessedObjects(int(float64(used) / 100. * 5))
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
					err = outerStore.Delete(h)
					if err != nil {
						logrus.Errorf("error pruning %s: %s", h, errors.FullTrace(err))
						continue
					}
				}
			}()
		}
		wg.Wait()
	}
	return nil
}

// GetUsedSpace returns how many bytes are used in the partition hosting the path
func GetUsedSpace(path string) (int, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		return 0, errors.Err(err)
	}
	// Available blocks * size per block = available space in bytes
	all := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	used := all - free

	return int(used), nil
}
