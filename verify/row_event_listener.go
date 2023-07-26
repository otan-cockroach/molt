package verify

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/retry"
	"github.com/cockroachdb/molt/verify/inconsistency"
)

type RowEventListener interface {
	OnExtraneousRow(row inconsistency.ExtraneousRow)
	OnMissingRow(row inconsistency.MissingRow)
	OnMismatchingRow(row inconsistency.MismatchingRow)
	OnMatch()
	OnRowScan()
}

// defaultRowEventListener is the default invocation of the row event listener.
type defaultRowEventListener struct {
	reporter inconsistency.Reporter
	stats    rowStats
	table    TableShard
}

func (n *defaultRowEventListener) OnExtraneousRow(row inconsistency.ExtraneousRow) {
	n.reporter.Report(row)
	n.stats.numExtraneous++
}

func (n *defaultRowEventListener) OnMissingRow(row inconsistency.MissingRow) {
	n.stats.numMissing++
	n.reporter.Report(row)
}

func (n *defaultRowEventListener) OnMismatchingRow(row inconsistency.MismatchingRow) {
	n.reporter.Report(row)
	n.stats.numMismatch++
}

func (n *defaultRowEventListener) OnMatch() {
	n.stats.numSuccess++
}

func (n *defaultRowEventListener) OnRowScan() {
	if n.stats.numVerified%10000 == 0 && n.stats.numVerified > 0 {
		n.reporter.Report(inconsistency.StatusReport{
			Info: fmt.Sprintf("progress on %s.%s (shard %d/%d): %s", n.table.Schema, n.table.Table, n.table.ShardNum, n.table.TotalShards, n.stats.String()),
		})
	}
	n.stats.numVerified++
}

// liveRowEventListener is used when `live` mode is enabled.
type liveRowEventListener struct {
	base *defaultRowEventListener
	pks  []tree.Datums
	r    *Reverifier
}

func (n *liveRowEventListener) OnExtraneousRow(row inconsistency.ExtraneousRow) {
	n.pks = append(n.pks, row.PrimaryKeyValues)
	n.base.stats.numLiveRetry++
}

func (n *liveRowEventListener) OnMissingRow(row inconsistency.MissingRow) {
	n.pks = append(n.pks, row.PrimaryKeyValues)
	n.base.stats.numLiveRetry++
}

func (n *liveRowEventListener) OnMismatchingRow(row inconsistency.MismatchingRow) {
	n.pks = append(n.pks, row.PrimaryKeyValues)
	n.base.stats.numLiveRetry++
}

func (n *liveRowEventListener) OnMatch() {
	n.base.OnMatch()
}

func (n *liveRowEventListener) OnRowScan() {
	n.base.OnRowScan()
	if n.base.stats.numVerified%10000 == 0 {
		n.Flush()
	}
}

func (n *liveRowEventListener) Flush() {
	if len(n.pks) > 0 {
		r, err := retry.NewRetry(retry.Settings{
			InitialBackoff: time.Millisecond * 200,
			Multiplier:     4,
			MaxBackoff:     3 * time.Second,
			MaxRetries:     6,
		})
		if err != nil {
			panic(err)
		}
		n.r.Push(&RetryItem{
			PrimaryKeys: n.pks,
			// TODO: configurable.
			Retry: r,
		})
		n.pks = nil
	}
}
