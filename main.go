package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gody-cdn/configs"
	"gody-cdn/server/http"
	"gody-cdn/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"
	"github.com/sirupsen/logrus"
)

func main() {
	stopper := stop.New()
	err := configs.Init("config.json")
	if err != nil {
		logrus.Fatalln(errors.FullTrace(err))
	}
	s3Store := store.NewS3Store(configs.Configuration.S3Origins[0])
	err = os.MkdirAll(configs.Configuration.DiskCache.Path, os.ModePerm)
	if err != nil {
		logrus.Fatal(errors.FullTrace(err))
	}
	ds := store.NewDiskStore(configs.Configuration.DiskCache.Path, 2)
	localDB := configs.Configuration.LocalDB
	localDsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", localDB.User, localDB.Password, localDB.Host, localDB.Database)
	dbs := store.NewDBBackedStore(ds, localDsn)

	go selfCleanup(dbs, dbs, stopper, configs.Configuration.DiskCache)

	finalStore := store.NewCachingStore("nvme-db-store", s3Store, dbs)
	defer finalStore.Shutdown()

	httpServer := http.NewServer(finalStore, 4000)
	err = httpServer.Start(":" + strconv.Itoa(2222))
	if err != nil {
		logrus.Fatal(err)
	}
	defer httpServer.Shutdown()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	// deferred shutdowns happen now
	stopper.StopAndWait()
}

func selfCleanup(dbStore *store.DBBackedStore, outerStore store.ObjectStore, stopper *stop.Group, diskConfig configs.ObjectCacheParams) {
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
