package http

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/OdyseeTeam/gody-cdn/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/reflector.go/shared"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func (s *Server) getObject(c *gin.Context) {
	waiter := &sync.WaitGroup{}
	waiter.Add(1)
	enqueue(&blobRequest{c: c, finished: waiter})
	waiter.Wait()
}

func (s *Server) HandleGetObject(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Recovered from panic: %v", r)
		}
	}()
	start := time.Now()
	objectName := strings.ReplaceAll(c.Request.RequestURI, "/t-na/", "")
	log.Debugf("object name: %s", objectName)
	if s.missesCache.Has(objectName) {
		serialized, err := shared.NewBlobTrace(time.Since(start), "http").Serialize()
		c.Header("Via", serialized)
		if err != nil {
			_ = c.Error(errors.Err(err))
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	blob, trace, err := s.store.Get(objectName, nil)
	if err != nil {
		serialized, serializeErr := trace.Serialize()
		if serializeErr != nil {
			_ = c.Error(errors.Prefix(serializeErr.Error(), err))
			c.String(http.StatusInternalServerError, errors.Prefix(serializeErr.Error(), err).Error())
			return
		}
		c.Header("Via", serialized)

		if errors.Is(err, store.ErrObjectNotFound) {
			_ = s.missesCache.Set(objectName, true)
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		_ = c.Error(err)
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	serialized, err := trace.Serialize()
	if err != nil {
		_ = c.Error(err)
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	parts := strings.Split(objectName, "/")
	actualFileName := parts[len(parts)-1]
	c.Header("Via", serialized)
	c.Header("Content-Disposition", "filename="+actualFileName)
	c.Data(http.StatusOK, "application/octet-stream", blob)
}

func (s *Server) hasObject(c *gin.Context) {
	objectName := c.Query("object")
	has, err := s.store.Has(objectName)
	if err != nil {
		_ = c.Error(err)
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if has {
		c.Status(http.StatusNoContent)
		return
	}
	c.Status(http.StatusNotFound)
}

func (s *Server) recoveryHandler(c *gin.Context, err interface{}) {
	c.JSON(500, gin.H{
		"title": "Error",
		"err":   err,
	})
}
