package verify

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/retry"
)

type RowEventListener interface {
	OnExtraneousRow(row ExtraneousRow)
	OnMissingRow(row MissingRow)
	OnMismatchingRow(row MismatchingRow)
	OnMatch()
	OnRowScan()
}

// defaultRowEventListener is the default invocation of the row event listener.
type defaultRowEventListener struct {
	reporter Reporter
	stats    rowStats
	table    TableShard
}

func (n *defaultRowEventListener) OnExtraneousRow(row ExtraneousRow) {
	n.reporter.Report(row)
	n.stats.numExtraneous++
}

func (n *defaultRowEventListener) OnMissingRow(row MissingRow) {
	n.stats.numMissing++
	n.reporter.Report(row)
}

func (n *defaultRowEventListener) OnMismatchingRow(row MismatchingRow) {
	n.reporter.Report(row)
	n.stats.numMismatch++
}

func (n *defaultRowEventListener) OnMatch() {
	n.stats.numSuccess++
}

func (n *defaultRowEventListener) OnRowScan() {
	if n.stats.numVerified%10000 == 0 && n.stats.numVerified > 0 {
		n.reporter.Report(StatusReport{
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

func (n *liveRowEventListener) OnExtraneousRow(row ExtraneousRow) {
	n.pks = append(n.pks, row.PrimaryKeyValues)
	n.base.stats.numLiveRetry++
}

func (n *liveRowEventListener) OnMissingRow(row MissingRow) {
	n.pks = append(n.pks, row.PrimaryKeyValues)
	n.base.stats.numLiveRetry++
}

func (n *liveRowEventListener) OnMismatchingRow(row MismatchingRow) {
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
