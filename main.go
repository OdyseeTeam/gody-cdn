package main

import (
	"fmt"
	"os"

	"gody-cdn/configs"
	"gody-cdn/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/sirupsen/logrus"
)

func main() {
	err := configs.Init("config.json")
	if err != nil {
		logrus.Fatalln(errors.FullTrace(err))
	}

	err = os.MkdirAll(configs.Configuration.DiskCache.Path, os.ModePerm)
	if err != nil {
		logrus.Fatal(errors.FullTrace(err))
	}
	ds := store.NewDiskStore(configs.Configuration.DiskCache.Path, 2)
	localDB := configs.Configuration.LocalDB
	localDsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", localDB.User, localDB.Password, localDB.Host, localDB.Database)
	dbs := store.NewDBBackedStore(ds, localDsn)
	err = dbs.Put("test", []byte("test2"))
	if err != nil {
		logrus.Fatalln(errors.FullTrace(err))
	}
	object, trace, err := dbs.Get("test")
	if err != nil {
		logrus.Fatalln(errors.FullTrace(err))
	}
	logrus.Infoln(trace.String())
	logrus.Infof("Object nil: %t", object == nil)
	ds.Shutdown()
}
