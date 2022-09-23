package configs

import (
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/c2h5oh/datasize"
	"github.com/sirupsen/logrus"
	"github.com/tkanos/gonfig"
)

type DbConfig struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Database string `json:"database"`
	Password string `json:"password"`
}

type S3Configs struct {
	ID       string `json:"id"`
	Secret   string `json:"secret"`
	Region   string `json:"region"`
	Bucket   string `json:"bucket"`
	Endpoint string `json:"endpoint"`
}
type ObjectCacheParams struct {
	Path string `json:"path"`
	Size string `json:"size"`
}

type Configs struct {
	SlackToken             string            `json:"slack_token"`
	S3Origins              []S3Configs       `json:"s3_origins"`
	LocalDB                DbConfig          `json:"local_db"`
	DiskCache              ObjectCacheParams `json:"disk_cache"`
	CleanupIntervalSeconds int               `json:"cleanup_interval_seconds"`
}

var Configuration *Configs

func Init(configPath string) error {
	if Configuration != nil {
		return nil
	}
	c := Configs{}
	err := gonfig.GetConf(configPath, &c)
	if err != nil {
		return errors.Err(err)
	}
	Configuration = &c
	return nil
}

func (o *ObjectCacheParams) GetMaxSize() int {
	var maxSize datasize.ByteSize
	err := maxSize.UnmarshalText([]byte(o.Size))
	if err != nil {
		logrus.Fatalf(errors.FullTrace(err))
	}
	if maxSize <= 0 {
		logrus.Fatalf("disk cache size for \"%s\" must be more than 0. Parsed: %dB", o.Path, maxSize)
	}
	return int(maxSize)
}

func (s *S3Configs) GetS3AWSConfig() *aws.Config {
	return &aws.Config{
		Credentials:      credentials.NewStaticCredentials(s.ID, s.Secret, ""),
		Region:           &s.Region,
		Endpoint:         &s.Endpoint,
		S3ForcePathStyle: aws.Bool(true),
	}
}

func (c *Configs) GetCleanupInterval() time.Duration {
	return time.Duration(c.CleanupIntervalSeconds) * time.Second
}
