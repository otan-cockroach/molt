package datamovestore

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
)

type s3Store struct {
	logger  zerolog.Logger
	bucket  string
	session *session.Session
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
) (string, error) {
	fileName := fmt.Sprintf("%s/part_%08d.csv", table.SafeString(), iteration)
	creds, err := s.session.Config.Credentials.Get()
	if err != nil {
		return "", err
	}
	s.logger.Debug().Str("file", fileName).Msgf("creating new file")
	if _, err := s3manager.NewUploader(s.session).UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fileName),
		Body:   r,
	}); err != nil {
		return fileName, err
	}
	s.logger.Debug().Str("file", fileName).Msgf("s3 file creation batch complete")
	return fmt.Sprintf(
		"s3://%s/%s?AWS_ACCESS_KEY_ID=%s&AWS_SECRET_ACCESS_KEY=%s",
		s.bucket,
		fileName,
		url.QueryEscape(creds.AccessKeyID),
		url.QueryEscape(creds.SecretAccessKey),
	), nil
}

func (s *s3Store) CanBeTarget() bool {
	return true
}

func (s *s3Store) DefaultFlushBatchSize() int {
	return 512 * 1024 * 1024
}
