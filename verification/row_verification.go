package verification

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/rowiterator"
	"github.com/lib/pq/oid"
	"github.com/rs/zerolog"
)

type rowStats struct {
	numVerified   int
	numSuccess    int
	numMissing    int
	numMismatch   int
	numExtraneous int
}

func (s *rowStats) String() string {
	return fmt.Sprintf(
		"truth rows seen: %d, success: %d, missing: %d, mismatch: %d, extraneous: %d",
		s.numVerified,
		s.numSuccess,
		s.numMissing,
		s.numMismatch,
		s.numExtraneous,
	)
}

// compareContext implements tree.CompareContext
type compareContext struct{}

func (c *compareContext) UnwrapDatum(d tree.Datum) tree.Datum {
	return d
}

func (c *compareContext) GetLocation() *time.Location {
	return time.UTC
}

func (c *compareContext) GetRelativeParseTime() time.Time {
	return time.Now().UTC()
}

func (c *compareContext) MustGetPlaceholderValue(p *tree.Placeholder) tree.Datum {
	return p
}

type TableShard struct {
	Schema                 tree.Name
	Table                  tree.Name
	MatchingColumns        []tree.Name
	MatchingColumnTypeOIDs [2][]oid.Oid
	PrimaryKeyColumns      []tree.Name
	StartPKVals            []tree.Datum
	EndPKVals              []tree.Datum

	ShardNum    int
	TotalShards int
}

func verifyRowsOnShard(
	ctx context.Context,
	conns dbconn.OrderedConns,
	table TableShard,
	rowBatchSize int,
	reporter Reporter,
	logger zerolog.Logger,
) error {
	var iterators [2]rowiterator.Iterator
	for i, conn := range conns {
		var err error
		iterators[i], err = rowiterator.NewScanIterator(
			ctx,
			conn,
			rowiterator.Table{
				Schema:            table.Schema,
				Table:             table.Table,
				ColumnNames:       table.MatchingColumns,
				ColumnOIDs:        table.MatchingColumnTypeOIDs[i],
				PrimaryKeyColumns: table.PrimaryKeyColumns,
				StartPKVals:       table.StartPKVals,
				EndPKVals:         table.EndPKVals,
			},
			rowBatchSize,
		)
		if err != nil {
			return errors.Wrapf(err, "error initializing row iterator on %s", conn.ID())
		}
	}

	var evl VerifyEventListener = &nonLiveReporter{reporter: reporter, table: table}
	if err := verifyRows(ctx, iterators, table, evl); err != nil {
		return err
	}
	switch evl := evl.(type) {
	case *nonLiveReporter:
		reporter.Report(StatusReport{
			Info: fmt.Sprintf("finished row verification on %s.%s (shard %d/%d): %s", table.Schema, table.Table, table.ShardNum, table.TotalShards, evl.stats.String()),
		})
	}
	return nil
}

func verifyRows(
	ctx context.Context, iterators [2]rowiterator.Iterator, table TableShard, evl VerifyEventListener,
) error {
	cmpCtx := &compareContext{}

	truth := iterators[0]
	for truth.HasNext(ctx) {
		evl.OnRowScan()

		truthVals := truth.Next(ctx)
		it := iterators[1]

	itLoop:
		for {
			if !it.HasNext(ctx) && it.Error() == nil {
				evl.OnMissingRow(MissingRow{
					ConnID:            it.Conn().ID(),
					Schema:            table.Schema,
					Table:             table.Table,
					PrimaryKeyColumns: table.PrimaryKeyColumns,
					PrimaryKeyValues:  truthVals[:len(table.PrimaryKeyColumns)],
					Columns:           table.MatchingColumns,
					Values:            truthVals,
				})
				break
			}

			// Check the primary key.
			targetVals := it.Peek(ctx)
			var compareVal int
			for i := range table.PrimaryKeyColumns {
				if compareVal = truthVals[i].Compare(cmpCtx, targetVals[i]); compareVal != 0 {
					break
				}
			}
			switch compareVal {
			case 1:
				// Extraneous row. Log and continue.
				it.Next(ctx)
				evl.OnExtraneousRow(ExtraneousRow{
					ConnID:            it.Conn().ID(),
					Schema:            table.Schema,
					Table:             table.Table,
					PrimaryKeyColumns: table.PrimaryKeyColumns,
					PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
				})
			case 0:
				// Matching primary key. Compare values and break loop.
				targetVals = it.Next(ctx)
				mismatches := MismatchingRow{
					ConnID:            it.Conn().ID(),
					Schema:            table.Schema,
					Table:             table.Table,
					PrimaryKeyColumns: table.PrimaryKeyColumns,
					PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
				}
				for valIdx := len(table.PrimaryKeyColumns); valIdx < len(targetVals); valIdx++ {
					if targetVals[valIdx].Compare(cmpCtx, truthVals[valIdx]) != 0 {
						mismatches.MismatchingColumns = append(mismatches.MismatchingColumns, table.MatchingColumns[valIdx])
						mismatches.TargetVals = append(mismatches.TargetVals, targetVals[valIdx])
						mismatches.TruthVals = append(mismatches.TruthVals, truthVals[valIdx])
					}
				}
				if len(mismatches.MismatchingColumns) > 0 {
					evl.OnMismatchingRow(mismatches)
				} else {
					evl.OnMatch()
				}
				break itLoop
			case -1:
				evl.OnMissingRow(MissingRow{
					ConnID:            it.Conn().ID(),
					Schema:            table.Schema,
					Table:             table.Table,
					PrimaryKeyColumns: table.PrimaryKeyColumns,
					PrimaryKeyValues:  truthVals[:len(table.PrimaryKeyColumns)],
					Columns:           table.MatchingColumns,
					Values:            truthVals,
				})
				break itLoop
			}
		}
	}

	for _, it := range iterators[1:] {
		if err := it.Error(); err != nil {
			return err
		}

		for it.HasNext(ctx) {
			targetVals := it.Next(ctx)
			evl.OnExtraneousRow(ExtraneousRow{
				ConnID:            it.Conn().ID(),
				Schema:            table.Schema,
				Table:             table.Table,
				PrimaryKeyColumns: table.PrimaryKeyColumns,
				PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
			})
		}
	}

	return nil
}

type VerifyEventListener interface {
	OnExtraneousRow(row ExtraneousRow)
	OnMissingRow(row MissingRow)
	OnMismatchingRow(row MismatchingRow)
	OnMatch()
	OnRowScan()
}

type nonLiveReporter struct {
	reporter Reporter
	stats    rowStats
	table    TableShard
}

func (n *nonLiveReporter) OnExtraneousRow(row ExtraneousRow) {
	n.reporter.Report(row)
	n.stats.numExtraneous++
}

func (n *nonLiveReporter) OnMissingRow(row MissingRow) {
	n.stats.numMissing++
	n.reporter.Report(row)
}

func (n *nonLiveReporter) OnMismatchingRow(row MismatchingRow) {
	n.reporter.Report(row)
	n.stats.numMismatch++
}

func (n *nonLiveReporter) OnMatch() {
	n.stats.numSuccess++
}

func (n *nonLiveReporter) OnRowScan() {
	if n.stats.numVerified%10000 == 0 && n.stats.numVerified > 0 {
		n.reporter.Report(StatusReport{
			Info: fmt.Sprintf("progress on %s.%s (shard %d/%d): %s", n.table.Schema, n.table.Table, n.table.ShardNum, n.table.TotalShards, n.stats.String()),
		})
	}
	n.stats.numVerified++
}
