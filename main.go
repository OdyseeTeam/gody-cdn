package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

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

	finalStore := store.NewCachingStore("nvme-db-store", s3Store, dbs)
	defer finalStore.Shutdown()

	httpServer := http.NewServer(finalStore, 200)
	err = httpServer.Start(":" + strconv.Itoa(2222))
	if err != nil {
		logrus.Fatal(err)
	}
	defer httpServer.Shutdown()

	//object, trace, err := finalStore.Get("2f62c5e710980d944e353abb093dff96079e92de86ecf993ab1da332eac235e42b681887e00b8fec32a68a6e118292e7/seg_0_000017.ts")
	//if err != nil {
	//	logrus.Fatalln(errors.FullTrace(err))
	//}
	//logrus.Infoln(trace.String())
	//logrus.Infof("Object nil: %t", object == nil)
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	<-interruptChan
	// deferred shutdowns happen now
	stopper.StopAndWait()
}
