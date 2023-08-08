package datamove

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove/dataexport"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

type ExportResult struct {
	Resources []datamovestore.Resource
	CDCCursor string
	StartTime time.Time
	EndTime   time.Time
	NumRows   int
}

func Export(
	ctx context.Context,
	logger zerolog.Logger,
	sqlSrc dataexport.Source,
	datasource datamovestore.Store,
	table dbtable.VerifiedTable,
	flushSize int,
) (ExportResult, error) {
	ret := ExportResult{
		StartTime: time.Now(),
	}
	ret.CDCCursor = sqlSrc.CDCCursor()
	logger.Debug().
		Str("cdc_cursor", ret.CDCCursor).
		Msgf("established approriate cdc cursor location")

	cancellableCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	sqlRead, sqlWrite := io.Pipe()
	// Run the COPY TO, which feeds into the pipe, concurrently.
	copyWG, _ := errgroup.WithContext(ctx)
	copyWG.Go(func() error {
		sqlSrcConn, err := sqlSrc.Conn(ctx)
		if err != nil {
			return err
		}
		return errors.CombineErrors(
			func() error {
				if err := sqlSrcConn.Export(cancellableCtx, sqlWrite, table); err != nil {
					return errors.CombineErrors(err, sqlWrite.CloseWithError(err))
				}
				return sqlWrite.Close()
			}(),
			sqlSrcConn.Close(ctx),
		)
	})

	var resourceWG sync.WaitGroup
	itNum := 0
	// Errors must be buffered, as pipe can exit without taking the error channel.
	writerErrCh := make(chan error, 1)
	pipe := newCSVPipe(sqlRead, logger, flushSize, func() io.WriteCloser {
		resourceWG.Wait()
		forwardRead, forwardWrite := io.Pipe()
		resourceWG.Add(1)
		go func() {
			defer resourceWG.Done()
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

	err := pipe.Pipe(table.Name)
	// Wait for the resource wait group to complete. It may output an error
	// that is not captured in the pipe.
	resourceWG.Wait()
	// Check any errors are not left behind - this can happen if the data source
	// creation fails, but the COPY is already done.
	select {
	case werr := <-writerErrCh:
		if werr != nil {
			cancelFunc()
			err = errors.CombineErrors(err, werr)
		}
	default:
	}
	if err != nil {
		// We do not wait for COPY to complete - we're already in trouble.
		return ret, err
	}

	ret.NumRows = pipe.numRows
	ret.EndTime = time.Now()
	return ret, copyWG.Wait()
}
