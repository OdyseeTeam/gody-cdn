package http

import (
	"sync"

	"github.com/lbryio/lbry.go/v2/extras/stop"

	"github.com/gin-gonic/gin"
)

type blobRequest struct {
	c        *gin.Context
	finished *sync.WaitGroup
}

var getReqCh = make(chan *blobRequest, 20000)

func InitWorkers(server *Server, workers int) {
	stopper := stop.New(server.grp)
	for i := 0; i < workers; i++ {
		go func(worker int) {
			for {
				select {
				case <-stopper.Ch():
				case r := <-getReqCh:
					process(server, r)
				}
			}
		}(i)
	}
}

func enqueue(b *blobRequest) {
	getReqCh <- b
}

func process(server *Server, r *blobRequest) {
	server.HandleGetObject(r.c)
	r.finished.Done()
}
