package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/OdyseeTeam/gody-cdn/cleanup"
	"github.com/OdyseeTeam/gody-cdn/configs"
	"github.com/OdyseeTeam/gody-cdn/server/http"
	"github.com/OdyseeTeam/gody-cdn/store"

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
	s3Stores, err := store.NewMultiS3Store(configs.Configuration.S3Origins)
	if err != nil {
		logrus.Fatalln(errors.FullTrace(err))
	}
	err = os.MkdirAll(configs.Configuration.DiskCache.Path, os.ModePerm)
	if err != nil {
		logrus.Fatal(errors.FullTrace(err))
	}
	ds, err := store.NewDiskStore(configs.Configuration.DiskCache.Path, 2)
	if err != nil {
		logrus.Fatal(errors.FullTrace(err))
	}
	localDB := configs.Configuration.LocalDB
	localDsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", localDB.User, localDB.Password, localDB.Host, localDB.Database)
	dbs := store.NewDBBackedStore(ds, localDsn)

	go cleanup.SelfCleanup(dbs, dbs, stopper, configs.Configuration.DiskCache)

	finalStore := store.NewCachingStore("nvme-db-store", s3Stores, dbs)
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
