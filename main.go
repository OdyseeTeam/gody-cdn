package main

import (
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
	ds.Shutdown()
}
