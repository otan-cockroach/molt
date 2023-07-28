package datamove

import (
	"context"
	"fmt"
	"io"
	"sync"
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

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// Run the pipe from SQL to the relevant data store concurrently.
	errorCh := make(chan error)
	sqlRead, sqlWrite := io.Pipe()
	go func() {
		defer close(errorCh)

		itNum := 0
		writerErrorCh := make(chan error)
		flushSize := 512 * 1024 * 1024
		if targetCopyConn != nil {
			flushSize = 4 * 1024 * 1024
		}
		var runWG sync.WaitGroup
		pipe := newCSVPipe(sqlRead, flushSize, func() io.WriteCloser {
			forwardRead, forwardWrite := io.Pipe()
			go func() {
				runWG.Wait()
				runWG.Add(1)
				defer runWG.Done()
				itNum++
				if err := func() error {
					if targetCopyConn != nil {
						target := targetCopyConn.(*dbconn.PGConn)
						if _, err := target.PgConn().CopyFrom(ctx, forwardRead, "COPY "+table.SafeString()+" FROM STDIN CSV"); err != nil {
							return err
						}
						logger.Debug().Int("batch", itNum).Msgf("csv batch complete")
					} else {
						fileName := fmt.Sprintf("%s/part_%08d.csv", table.SafeString(), itNum)
						logger.Debug().Str("file", fileName).Msgf("creating new file")
						ret.Files = append(ret.Files, fileName)
						if _, err := uploader.Upload(&s3manager.UploadInput{
							Bucket: aws.String(bucket),
							Key:    aws.String(fileName),
							Body:   forwardRead,
						}); err != nil {
							return err
						}
						logger.Debug().Str("file", fileName).Msgf("s3 file creation batch complete")
					}
					return nil
				}(); err != nil {
					logger.Err(err).Msgf("error during write")
					if err := forwardRead.Close(); err != nil {
						logger.Err(err).Msgf("error closing write goroutine")
					}
					writerErrorCh <- err
					return
				}
			}()
			return forwardWrite
		})
		select {
		case err := <-writerErrorCh:
			cancelFunc()
			errorCh <- err
		case errorCh <- pipe.Pipe():
		}
	}()

	// Run the COPY TO, which feeds into the pipe concurrently as well.
	copyFinishCh := make(chan error)
	go func() {
		defer close(copyFinishCh)
		if err := func() error {
			if _, err := tx.Conn().PgConn().CopyTo(cancellableCtx, sqlWrite, "COPY "+table.SafeString()+" TO STDOUT CSV"); err != nil {
				return err
			}
			return sqlWrite.Close()
		}(); err != nil {
			copyFinishCh <- err
		}
	}()

	select {
	case err, ok := <-errorCh:
		if !ok {
			return ret, errors.AssertionFailedf("unexpected channel closure")
		}
		return ret, err
	case err, ok := <-copyFinishCh:
		if !ok {
			break
		}
		if err != nil {
			return ret, err
		}
	}
	ret.EndTime = time.Now()
	if targetCopyConn != nil {
		logger.Debug().
			Dur("duration", ret.EndTime.Sub(ret.StartTime)).
			Msgf("copied all rows successfully")
	} else {
		logger.Debug().
			Strs("files", ret.Files).
			Dur("duration", ret.EndTime.Sub(ret.StartTime)).
			Msgf("files stored in s3 successfully")
	}
	return ret, nil
}
