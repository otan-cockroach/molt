package verification

import (
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/retry"
	"github.com/lib/pq/oid"
	"github.com/rs/zerolog"
)

type RetryItem struct {
	Schema                 tree.Name
	Table                  tree.Name
	MatchingColumns        []tree.Name
	MatchingColumnTypeOIDs [2][]oid.Oid
	PrimaryKeyColumns      []tree.Name

	PrimaryKeys []tree.Datums
	Retry       *retry.Retry
}

type Reverifier struct {
	insertQueue chan *RetryItem
	done        chan struct{}
	logger      zerolog.Logger
}

func NewReverifier(logger zerolog.Logger) *Reverifier {
	r := &Reverifier{
		insertQueue: make(chan *RetryItem),
		logger:      logger,
	}
	go func() {
		defer close(r.done)
		var queue reverifyQueue
		noTrafficChannel := make(chan time.Time)
		for {
			// By default, block until we get work.
			var nextWorkCh <-chan time.Time = noTrafficChannel
			if len(queue) > 0 {
				// If we have work queued up, wait for it.
				nextWorkCh = time.After(queue[0].Retry.NextRetry.Sub(time.Now()))
			}
			select {
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
				// TODO: reverify
			}
		}
	}()
	return r
}

func (r *Reverifier) Push(it *RetryItem) {
	r.insertQueue <- it
}

func (r *Reverifier) Close() {
	close(r.insertQueue)
}
