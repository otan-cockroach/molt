package datamove

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
)

type ExportResult struct {
	Resources  []datamovestore.Resource
	SnapshotID string
	StartTime  time.Time
	EndTime    time.Time
	NumRows    int
}

func Export(
	ctx context.Context,
	logger zerolog.Logger,
	sqlSrc ExportSource,
	datasource datamovestore.Store,
	table dbtable.VerifiedTable,
	flushSize int,
) (ExportResult, error) {
	ret := ExportResult{
		StartTime: time.Now(),
	}
	ret.SnapshotID = sqlSrc.SnapshotID()
	logger.Debug().Str("snapshot", ret.SnapshotID).Msgf("establishing consistent snapshot")

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// Run the pipe from SQL to the relevant data store concurrently.
	pipeErrorCh := make(chan error)
	sqlRead, sqlWrite := io.Pipe()
	var runWG sync.WaitGroup
	go func() {
		itNum := 0
		// Errors must be buffered, as pipe can exit without taking the error channel.
		writerErrCh := make(chan error, 1)
		pipe := newCSVPipe(sqlRead, flushSize, func() io.WriteCloser {
			runWG.Wait()
			forwardRead, forwardWrite := io.Pipe()
			runWG.Add(1)
			go func() {
				defer runWG.Done()
				itNum++
				if err := func() error {
					resource, err := datasource.CreateFromReader(ctx, forwardRead, table, itNum)
					if err != nil {
						return err
					}
					ret.Resources = append(ret.Resources, resource)
					return nil
				}(); err != nil {
					logger.Err(err).Msgf("error during data store write")
					if err := forwardRead.CloseWithError(err); err != nil {
						logger.Err(err).Msgf("error closing write goroutine")
					}
					writerErrCh <- err
				}
			}()
			return forwardWrite
		})
		err := pipe.Pipe()
		// Wait for all routines to exit.
		runWG.Wait()
		// Check any errors are not left behind.
		select {
		case werr := <-writerErrCh:
			if werr != nil {
				cancelFunc()
				err = errors.CombineErrors(err, werr)
			}
		default:
		}
		if err != nil {
			ret.NumRows = pipe.numRows
		}
		pipeErrorCh <- err
	}()

	// Run the COPY TO, which feeds into the pipe, concurrently.
	copyFinishCh := make(chan error)
	go func() {
		if err := func() error {
			if err := sqlSrc.Export(cancellableCtx, sqlWrite, table); err != nil {
				return errors.CombineErrors(err, sqlWrite.CloseWithError(err))
			}
			return sqlWrite.Close()
		}(); err != nil {
			copyFinishCh <- err
		}
	}()

	// Wait until pipeErrorCh returns.
	var copyErr error
waitLoop:
	for {
		select {
		case err := <-pipeErrorCh:
			if err == nil {
				break waitLoop
			}
			return ret, err
		case err := <-copyFinishCh:
			logger.Err(err).Msgf("error during copy")
			copyErr = err
		}
	}
	if copyErr != nil {
		return ret, copyErr
	}
	ret.EndTime = time.Now()
	return ret, nil
}
