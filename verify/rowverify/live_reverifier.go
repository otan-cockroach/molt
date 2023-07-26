package rowverify

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/retry"
	"github.com/cockroachdb/molt/rowiterator"
	"github.com/cockroachdb/molt/verify/inconsistency"
	"github.com/rs/zerolog"
)

type LiveReverificationSettings struct {
	MaxBatchSize  int
	FlushInterval time.Duration
	RetrySettings retry.Settings
}

type liveRetryItem struct {
	PrimaryKeys []tree.Datums
	Retry       *retry.Retry
}

type liveReverifier struct {
	insertQueue  chan *liveRetryItem
	scanComplete chan struct{}
	done         chan struct{}
	logger       zerolog.Logger
	table        TableShard
}

func newLiveReverifier(
	ctx context.Context,
	logger zerolog.Logger,
	baseConns dbconn.OrderedConns,
	table TableShard,
	baseListener RowEventListener,
) (*liveReverifier, error) {
	r := &liveReverifier{
		insertQueue:  make(chan *liveRetryItem),
		logger:       logger,
		table:        table,
		done:         make(chan struct{}),
		scanComplete: make(chan struct{}),
	}

	var conns dbconn.OrderedConns
	for i, conn := range baseConns {
		var err error
		conns[i], err = conn.Clone(ctx)
		if err != nil {
			return nil, err
		}
		// Initialize the type map on the connection.
		for _, typOID := range table.ColumnOIDs[i] {
			if _, err := dbconn.GetDataType(ctx, conn, typOID); err != nil {
				return nil, errors.Wrapf(err, "error initializing type oid %d", typOID)
			}
		}
	}

	go func() {
		defer func() {
			for _, conn := range conns {
				if err := conn.Close(ctx); err != nil {
					logger.Err(err).Msgf("error closing live reverifier connection")
				}
			}
			close(r.done)
		}()
		var queue liveRetryQueue
		noTrafficChannel := make(chan time.Time)
		var done bool
		for !done || len(queue.items) > 0 {
			if done {
				logger.Info().
					Int("num_batches", len(queue.items)).
					Int("num_pks", queue.numPKs).
					Msgf("waiting for live reverifier to complete")
			}
			// By default, block until we get work.
			var nextWorkCh <-chan time.Time = noTrafficChannel
			if len(queue.items) > 0 {
				// If we have work queued up, wait for it.
				nextWorkCh = time.After(time.Until(queue.items[0].Retry.NextRetry))
			}
			select {
			case <-r.scanComplete:
				done = true
				logger.Debug().Msgf("live reverifier marked scan as complete")
			case it, ok := <-r.insertQueue:
				// If the queue is closed, we can exit.
				if !ok {
					return
				}
				queue.heapPush(it)
			case <-nextWorkCh:
				it := queue.heapPop()
				if it == nil {
					continue
				}

				logger.Trace().
					Int("iteration", it.Retry.Iteration).
					Time("start_time", it.Retry.StartTime).
					Time("next_retry", it.Retry.NextRetry).
					Int("num_failed_keys", len(it.PrimaryKeys)).
					Msgf("live reverifying primary keys")
				var iterators [2]rowiterator.Iterator
				for i, conn := range conns {
					iterators[i] = rowiterator.NewPointLookupIterator(
						conn,
						rowiterator.Table{
							TableName:         table.TableName,
							ColumnNames:       table.Columns,
							ColumnOIDs:        table.ColumnOIDs[i],
							PrimaryKeyColumns: table.PrimaryKeyColumns,
						},
						it.PrimaryKeys,
					)
				}
				it.PrimaryKeys = it.PrimaryKeys[:0]
				if err := verifyRows(ctx, iterators, table, &reverifyEventListener{RetryItem: it, BaseListener: baseListener}); err != nil {
					logger.Err(err).Msgf("error during live verification")
					continue
				}
				if len(it.PrimaryKeys) > 0 {
					it.Retry.Next()
					queue.heapPush(it)
					logger.Trace().
						Int("iteration", it.Retry.Iteration-1).
						Time("start_time", it.Retry.StartTime).
						Time("next_retry", it.Retry.NextRetry).
						Int("num_failed_keys", len(it.PrimaryKeys)).
						Msgf("did not reverify everything; retrying")
				}
			}
		}
	}()
	return r, nil
}

func (r *liveReverifier) Push(it *liveRetryItem) {
	r.insertQueue <- it
}

func (r *liveReverifier) ScanComplete() {
	r.scanComplete <- struct{}{}
}

func (r *liveReverifier) WaitForDone() {
	<-r.done
}

type reverifyEventListener struct {
	RetryItem    *liveRetryItem
	BaseListener RowEventListener
}

func (r *reverifyEventListener) OnExtraneousRow(row inconsistency.ExtraneousRow) {
	if !r.RetryItem.Retry.ShouldContinue() {
		r.BaseListener.OnExtraneousRow(row)
	} else {
		r.RetryItem.PrimaryKeys = append(r.RetryItem.PrimaryKeys, row.PrimaryKeyValues)
	}
}

func (r *reverifyEventListener) OnMissingRow(row inconsistency.MissingRow) {
	if !r.RetryItem.Retry.ShouldContinue() {
		r.BaseListener.OnMissingRow(row)
	} else {
		r.RetryItem.PrimaryKeys = append(r.RetryItem.PrimaryKeys, row.PrimaryKeyValues)
	}
}

func (r *reverifyEventListener) OnMismatchingRow(row inconsistency.MismatchingRow) {
	if !r.RetryItem.Retry.ShouldContinue() {
		r.BaseListener.OnMismatchingRow(row)
	} else {
		r.RetryItem.PrimaryKeys = append(r.RetryItem.PrimaryKeys, row.PrimaryKeyValues)
	}
}

func (r *reverifyEventListener) OnMatch() {
	r.BaseListener.OnMatch()
}

func (r *reverifyEventListener) OnRowScan() {}
