package datamove

import (
	"context"
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
	Location   string
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

	type s3Result struct {
		out *s3manager.UploadOutput
		err error
	}
	resultCh := make(chan s3Result)
	r, w := io.Pipe()
	go func() {
		result, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(table.SafeString() + ".dump"),
			Body:   r,
		})
		resultCh <- s3Result{out: result, err: err}
	}()
	if _, err := tx.Conn().PgConn().CopyTo(ctx, w, "COPY "+table.SafeString()+" TO STDOUT CSV"); err != nil {
		return ret, errors.Wrap(err, "failed to export to stdout")
	}
	if err := w.Close(); err != nil {
		return ret, err
	}
	resultS := <-resultCh
	ret.EndTime = time.Now()
	result, err := resultS.out, resultS.err
	if err != nil {
		return ret, err
	}
	logger.Debug().
		Str("location", result.Location).
		Dur("duration", ret.EndTime.Sub(ret.StartTime)).
		Msgf("s3 stored")
	ret.Location = result.Location
	return ret, nil
}
