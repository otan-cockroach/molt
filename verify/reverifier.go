package verify

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/retry"
	"github.com/cockroachdb/molt/verify/internal/rowiterator"
	"github.com/rs/zerolog"
)

type RetryItem struct {
	PrimaryKeys []tree.Datums
	Retry       *retry.Retry
}

type Reverifier struct {
	insertQueue  chan *RetryItem
	scanComplete chan struct{}
	done         chan struct{}
	logger       zerolog.Logger
	table        TableShard
}

func NewReverifier(
	ctx context.Context,
	logger zerolog.Logger,
	baseConns dbconn.OrderedConns,
	table TableShard,
	baseListener VerifyEventListener,
) (*Reverifier, error) {
	r := &Reverifier{
		insertQueue:  make(chan *RetryItem),
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
		for _, typOID := range table.MatchingColumnTypeOIDs[i] {
			if _, err := dbconn.GetDataType(ctx, conn, typOID); err != nil {
				return nil, errors.Wrapf(err, "error initializing type oid %d", typOID)
			}
		}
	}

	go func() {
		defer func() {
			for _, conn := range conns {
				if err := conn.Close(ctx); err != nil {
					logger.Err(err).Msgf("error closing reverifier connection")
				}
			}
			close(r.done)
		}()
		var queue reverifyQueue
		noTrafficChannel := make(chan time.Time)
		var done bool
		for !done || len(queue) > 0 {
			if done {
				logger.Debug().Msgf("waiting for %d remaining items in reverifier queue", len(queue))
			}
			// By default, block until we get work.
			var nextWorkCh <-chan time.Time = noTrafficChannel
			if len(queue) > 0 {
				// If we have work queued up, wait for it.
				nextWorkCh = time.After(time.Until(queue[0].Retry.NextRetry))
			}
			select {
			case <-r.scanComplete:
				done = true
				logger.Debug().Msgf("reverifier marked scan as complete")
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

				logger.Debug().Msgf("reverifying primary keys (queued instance %s, iteration %d)", it.Retry.StartTime.Format(time.RFC3339), it.Retry.Iteration)
				var iterators [2]rowiterator.Iterator
				for i, conn := range conns {
					iterators[i] = rowiterator.NewPointLookupIterator(
						ctx,
						conn,
						rowiterator.Table{
							Schema:            table.Schema,
							Table:             table.Table,
							ColumnNames:       table.MatchingColumns,
							ColumnOIDs:        table.MatchingColumnTypeOIDs[i],
							PrimaryKeyColumns: table.PrimaryKeyColumns,
						},
						10,
						it.PrimaryKeys,
					)
					// TODO: make configurable row batch size
				}
				it.PrimaryKeys = it.PrimaryKeys[:0]
				if err := verifyRows(ctx, iterators, table, &reverifyEventListener{RetryItem: it, BaseListener: baseListener}); err != nil {
					logger.Err(err).Msgf("error during reverify")
					continue
				}
				if len(it.PrimaryKeys) > 0 {
					// dunno if this is abuse of retry, fix later
					it.Retry.Next()
					queue.heapPush(it)
					logger.Debug().Msgf("sending for another verification run at %s", it.Retry.NextRetry.Format(time.RFC3339))
				}
			}
		}
	}()
	return r, nil
}

func (r *Reverifier) Push(it *RetryItem) {
	r.insertQueue <- it
}

func (r *Reverifier) ScanComplete() {
	r.scanComplete <- struct{}{}
}

func (r *Reverifier) WaitForDone() {
	<-r.done
}

type reverifyEventListener struct {
	RetryItem    *RetryItem
	BaseListener VerifyEventListener
}

func (r *reverifyEventListener) OnExtraneousRow(row ExtraneousRow) {
	if !r.RetryItem.Retry.ShouldContinue() {
		r.BaseListener.OnExtraneousRow(row)
	} else {
		r.RetryItem.PrimaryKeys = append(r.RetryItem.PrimaryKeys, row.PrimaryKeyValues)
	}
}

func (r *reverifyEventListener) OnMissingRow(row MissingRow) {
	if !r.RetryItem.Retry.ShouldContinue() {
		r.BaseListener.OnMissingRow(row)
	} else {
		r.RetryItem.PrimaryKeys = append(r.RetryItem.PrimaryKeys, row.PrimaryKeyValues)
	}
}

func (r *reverifyEventListener) OnMismatchingRow(row MismatchingRow) {
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
