package datamove

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
)

type ExportResult struct {
	Files      []string
	SnapshotID string
	StartTime  time.Time
	EndTime    time.Time
}

func Export(
	ctx context.Context,
	baseConn dbconn.Conn,
	logger zerolog.Logger,
	bucket string,
	table dbtable.Name,
	targetCopyConn dbconn.Conn,
) (ExportResult, error) {
	conn := baseConn.(*dbconn.PGConn)
	ret := ExportResult{
		StartTime: time.Now(),
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return ret, err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&ret.SnapshotID); err != nil {
		return ret, errors.Wrap(err, "failed to export snapshot")
	}
	logger.Debug().Str("snapshot", ret.SnapshotID).Msgf("read a snapshot")
	sess := session.Must(session.NewSession())
	uploader := s3manager.NewUploader(sess)

	errorCh := make(chan error)
	sqlRead, sqlWrite := io.Pipe()
	prevClosed := make(chan struct{})
	go func() {
		fileIt := 1
		writerErrorCh := make(chan error)
		pipe := newCSVPipe(sqlRead, 512*1024*1024, func() io.WriteCloser {
			forwardRead, forwardWrite := io.Pipe()
			go func() {
				<-prevClosed
				if targetCopyConn != nil {
					target := targetCopyConn.(*dbconn.PGConn)
					if _, err := target.PgConn().CopyFrom(ctx, forwardRead, "COPY "+table.SafeString()+" FROM STDIN CSV"); err != nil {
						logger.Err(err).Msgf("error running COPY FROM STDIN")
						writerErrorCh <- err
						return
					}
					logger.Debug().Msgf("csv complete")
				} else {
					fileName := fmt.Sprintf("%s/part_%08d.csv", table.SafeString(), fileIt)
					logger.Debug().Str("file", fileName).Msgf("creating new file")
					ret.Files = append(ret.Files, fileName)
					fileIt++
					if _, err := uploader.Upload(&s3manager.UploadInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(fileName),
						Body:   forwardRead,
					}); err != nil {
						logger.Err(err).Msgf("error uploading to s3")
						writerErrorCh <- err
						return
					}
					logger.Debug().Str("file", fileName).Msgf("file creation complete")
				}
				prevClosed <- struct{}{}
			}()
			return forwardWrite
		})
		go func() {
			prevClosed <- struct{}{}
		}()
		select {
		case err := <-writerErrorCh:
			errorCh <- err
		case errorCh <- pipe.Pipe():
		}
	}()
	if _, err := tx.Conn().PgConn().CopyTo(ctx, sqlWrite, "COPY "+table.SafeString()+" TO STDOUT CSV"); err != nil {
		return ret, errors.Wrap(err, "failed to export to stdout")
	}
	if err := sqlWrite.Close(); err != nil {
		return ret, err
	}
	ret.EndTime = time.Now()
	if err := <-errorCh; err != nil {
		return ret, err
	}
	<-prevClosed
	if targetCopyConn != nil {
		logger.Debug().Msgf("copied all rows successfully")
	} else {
		logger.Debug().
			Strs("files", ret.Files).
			Dur("duration", ret.EndTime.Sub(ret.StartTime)).
			Msgf("files stored in s3 successfully")
	}
	return ret, nil
}
