package datamove

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove/datamovestore"
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
	datasource datamovestore.Store,
	table dbtable.Name,
	flushSize int,
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
	logger.Debug().Str("snapshot", ret.SnapshotID).Msgf("establishing consistent snapshot")

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// Run the pipe from SQL to the relevant data store concurrently.
	errorCh := make(chan error)
	sqlRead, sqlWrite := io.Pipe()
	var runWG sync.WaitGroup
	go func() {
		defer close(errorCh)

		itNum := 0
		writerErrorCh := make(chan error)
		pipe := newCSVPipe(sqlRead, flushSize, func() io.WriteCloser {
			runWG.Wait()
			forwardRead, forwardWrite := io.Pipe()
			go func() {
				runWG.Add(1)
				defer runWG.Done()
				itNum++
				if err := func() error {
					path, err := datasource.CreateFromReader(ctx, forwardRead, table, itNum)
					if err != nil {
						return err
					}
					ret.Files = append(ret.Files, path)
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
	runWG.Wait()
	logger.Info().
		Dur("duration", ret.EndTime.Sub(ret.StartTime)).
		Msgf("import phase complete")
	return ret, nil
}
