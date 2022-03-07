package store

import (
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/reflector.go/shared"
	"golang.org/x/sync/singleflight"
)

func WithSingleFlight(component string, origin ObjectStore) ObjectStore {
	return &singleFlightStore{
		ObjectStore: origin,
		component:   component,
		sf:          new(singleflight.Group),
	}
}

type singleFlightStore struct {
	ObjectStore

	component string
	sf        *singleflight.Group
}

func (s *singleFlightStore) Name() string {
	return "sf_" + s.ObjectStore.Name()
}

type getterResponse struct {
	object []byte
	stack  shared.BlobTrace
}

// Get ensures that only one request per hash is sent to the origin at a time,
// thereby protecting against https://en.wikipedia.org/wiki/Thundering_herd_problem
func (s *singleFlightStore) Get(hash string, extra interface{}) ([]byte, shared.BlobTrace, error) {
	start := time.Now()
	gr, err, _ := s.sf.Do(hash, s.getter(hash))
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), s.Name()), err
	}
	if gr == nil {
		return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err("getter response is nil")
	}
	rsp := gr.(getterResponse)
	return rsp.object, rsp.stack, nil
}

// getter returns a function that gets an object from the origin
// only one getter per hash will be executing at a time
func (s *singleFlightStore) getter(hash string) func() (interface{}, error) {
	return func() (interface{}, error) {
		start := time.Now()
		object, stack, err := s.ObjectStore.Get(hash, nil)
		if err != nil {
			return getterResponse{
				object: nil,
				stack:  stack.Stack(time.Since(start), s.Name()),
			}, err
		}

		return getterResponse{
			object: object,
			stack:  stack.Stack(time.Since(start), s.Name()),
		}, nil
	}
}

// Put ensures that only one request per hash is sent to the origin at a time,
// thereby protecting against https://en.wikipedia.org/wiki/Thundering_herd_problem
func (s *singleFlightStore) Put(hash string, object []byte) error {
	_, err, _ := s.sf.Do(hash, s.putter(hash, object))
	if err != nil {
		return err
	}
	return nil
}

// putter returns a function that puts an object from the origin
// only one putter per hash will be executing at a time
func (s *singleFlightStore) putter(hash string, object []byte) func() (interface{}, error) {
	return func() (interface{}, error) {
		err := s.ObjectStore.Put(hash, object)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
}

// Shutdown shuts down the store gracefully
func (s *singleFlightStore) Shutdown() {
	s.ObjectStore.Shutdown()
	return
}
