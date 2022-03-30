package store

import (
	"bytes"
	"net/http"
	"time"

	"github.com/OdyseeTeam/gody-cdn/configs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/reflector.go/shared"
	log "github.com/sirupsen/logrus"
)

// MultiS3Store is a collection of S3 stores
type MultiS3Store struct {
	instances []s3Instance
}
type s3Instance struct {
	config  configs.S3Configs
	session session.Session
}

// NewMultiS3Store returns an initialized S3 store pointer.
func NewMultiS3Store(configs []configs.S3Configs) (*MultiS3Store, error) {
	var ms MultiS3Store
	// we need to access the configs via index because session.NewSession does NOT copy the value of configs, rather stores the pointer only.
	for i := range configs {
		sess, err := session.NewSession(configs[i].GetS3AWSConfig())
		if err != nil {
			return nil, errors.Err(err)
		}
		ms.instances = append(ms.instances, s3Instance{
			config:  configs[i],
			session: *sess,
		})
	}

	return &ms, nil
}

type MultiS3Extras struct {
	S3Index int
}

const nameMultiS3 = "multiS3"

// Name is the cache type name
func (s *MultiS3Store) Name() string { return nameMultiS3 }

// Has returns T/F or Error ( from S3 ) if the store contains the object.
func (s *MultiS3Store) Has(hash string, extra interface{}) (bool, error) {
	ex := s.getExtras(extra)
	if ex == nil {
		return false, errors.Err("%s requires an origin index to be specified in the extra params. use the MultiS3Extras struct.", nameMultiS3)
	}
	_, err := s3.New(&s.instances[ex.S3Index].session).HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.instances[ex.S3Index].config.Bucket),
		Key:    aws.String(hash),
	})
	if err != nil {
		if reqFail, ok := err.(s3.RequestFailure); ok && reqFail.StatusCode() == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// Get returns the object slice if present or errors on S3.
func (s *MultiS3Store) Get(hash string, extra interface{}) ([]byte, shared.BlobTrace, error) {
	start := time.Now()
	ex := s.getExtras(extra)
	if ex == nil {
		return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err("%s requires an origin index to be specified in the extra params. use the MultiS3Extras struct.", nameMultiS3)
	}
	truncatedHash := hash
	if len(hash) > 8 {
		truncatedHash = hash[:8]
	}
	log.Debugf("Getting %s from S3 at index %d", truncatedHash, ex.S3Index)
	defer func(t time.Time) {
		log.Debugf("Getting %s from S3 took %s", truncatedHash, time.Since(t).String())
	}(start)

	buf := &aws.WriteAtBuffer{}
	_, err := s3manager.NewDownloader(&s.instances[ex.S3Index].session).Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(s.instances[ex.S3Index].config.Bucket),
		Key:    aws.String(hash),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err("bucket %s does not exist", s.instances[ex.S3Index].config.Bucket)
			case s3.ErrCodeNoSuchKey:
				return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err(ErrObjectNotFound)
			}
		}
	}
	return buf.Bytes(), shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err(err)
}

// Put stores the object on S3 or errors if S3 connection errors.
func (s *MultiS3Store) Put(hash string, object []byte, extra interface{}) error {
	ex := s.getExtras(extra)
	if ex == nil {
		return errors.Err("%s requires an origin index to be specified in the extra params. use the MultiS3Extras struct.", nameMultiS3)
	}
	log.Debugf("Uploading %s to S3", hash[:8])
	defer func(t time.Time) {
		log.Debugf("Uploading %s took %s", hash[:8], time.Since(t).String())
	}(time.Now())

	_, err := s3manager.NewUploader(&s.instances[ex.S3Index].session).Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.instances[ex.S3Index].config.Bucket),
		Key:    aws.String(hash),
		Body:   bytes.NewBuffer(object),
		ACL:    aws.String("public-read"),
	})
	return err
}

func (s *MultiS3Store) Delete(hash string, extra interface{}) error {
	ex := s.getExtras(extra)
	if ex == nil {
		return errors.Err("%s requires an origin index to be specified in the extra params. use the MultiS3Extras struct.", nameMultiS3)
	}
	log.Debugf("Deleting %s from S3", hash[:8])

	_, err := s3.New(&s.instances[ex.S3Index].session).DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.instances[ex.S3Index].config.Bucket),
		Key:    aws.String(hash),
	})

	return err
}

// Shutdown shuts down the store gracefully
func (s *MultiS3Store) Shutdown() {
}

func (s *MultiS3Store) getExtras(extra interface{}) *MultiS3Extras {
	ms, ok := extra.(MultiS3Extras)
	if !ok {
		return nil
	}
	return &ms
}
