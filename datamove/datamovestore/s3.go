package datamovestore

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
)

type s3Store struct {
	logger  zerolog.Logger
	bucket  string
	session *session.Session
}

type s3Resource struct {
	session *session.Session
	bucket  string
	key     string
}

func (s *s3Resource) ImportURL() (string, error) {
	creds, err := s.session.Config.Credentials.Get()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"s3://%s/%s?AWS_ACCESS_KEY_ID=%s&AWS_SECRET_ACCESS_KEY=%s",
		s.bucket,
		s.key,
		url.QueryEscape(creds.AccessKeyID),
		url.QueryEscape(creds.SecretAccessKey),
	), nil
}

func (s *s3Resource) Cleanup(ctx context.Context) error {
	return errors.Newf("unimplemented")
}

func NewS3Store(logger zerolog.Logger, session *session.Session, bucket string) *s3Store {
	return &s3Store{
		bucket:  bucket,
		session: session,
		logger:  logger,
	}
}

func (s *s3Store) CreateFromReader(
	ctx context.Context, r io.Reader, table dbtable.Name, iteration int,
) (Resource, error) {
	key := fmt.Sprintf("%s/part_%08d.csv", table.SafeString(), iteration)

	s.logger.Debug().Str("file", key).Msgf("creating new file")
	if _, err := s3manager.NewUploader(s.session).UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	}); err != nil {
		return nil, err
	}
	s.logger.Debug().Str("file", key).Msgf("s3 file creation batch complete")
	return &s3Resource{
		session: s.session,
		bucket:  s.bucket,
		key:     key,
	}, nil
}

func (s *s3Store) CanBeTarget() bool {
	return true
}

func (s *s3Store) DefaultFlushBatchSize() int {
	return 512 * 1024 * 1024
}
