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

var TimingEnabled = false

func compareRows(
	ctx context.Context,
	conns dbconn.OrderedConns,
	table TableShard,
	rowBatchSize int,
	reporter Reporter,
) error {
	startTime := time.Now()
	var iterators [2]*rowiterator.Iterator
	for i, conn := range conns {
		var err error
		iterators[i], err = rowiterator.NewIterator(
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

	cmpCtx := &compareContext{}

	var stats rowStats
	truth := iterators[0]
	for truth.HasNext(ctx) {
		if stats.numVerified%10000 == 0 && stats.numVerified > 0 {
			reporter.Report(StatusReport{
				Info: fmt.Sprintf("progress on %s.%s (shard %d/%d): %s", table.Schema, table.Table, table.ShardNum, table.TotalShards, stats.String()),
			})
			if TimingEnabled {
				endTime := time.Now()
				duration := endTime.Sub(startTime)
				reporter.Report(StatusReport{
					Info: fmt.Sprintf("scanned %d rows in %s (%.2f rows/sec)", stats.numVerified, duration, float64(stats.numVerified)/(float64(duration)/float64(time.Second))),
				})
			}
		}
		stats.numVerified++

		truthVals := truth.Next(ctx)
		for itIdx := 1; itIdx < len(iterators); itIdx++ {
			it := iterators[itIdx]

		itLoop:
			for {
				if !it.HasNext(ctx) && it.Error() == nil {
					stats.numMissing++
					reporter.Report(MissingRow{
						ConnID:            it.Conn.ID(),
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
					reporter.Report(ExtraneousRow{
						ConnID:            it.Conn.ID(),
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
					})
					stats.numExtraneous++
				case 0:
					// Matching primary key. compare values and break loop.
					targetVals = it.Next(ctx)
					mismatches := MismatchingRow{
						ConnID:            it.Conn.ID(),
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
						reporter.Report(mismatches)
						stats.numMismatch++
					} else {
						stats.numSuccess++
					}
					break itLoop
				case -1:
					// Missing a row.
					reporter.Report(MissingRow{
						ConnID:            it.Conn.ID(),
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  truthVals[:len(table.PrimaryKeyColumns)],
						Columns:           table.MatchingColumns,
						Values:            truthVals,
					})
					stats.numMissing++
					break itLoop
				}
			}
		}
	}

	for _, it := range iterators[1:] {
		if err := it.Error(); err != nil {
			reporter.Report(StatusReport{
				Info: fmt.Sprintf("error validating %s.%s on %s: %v", table.Schema, table.Table, it.Conn.ID(), err),
			})
			return err
		}

		for it.HasNext(ctx) {
			targetVals := it.Next(ctx)
			reporter.Report(ExtraneousRow{
				ConnID:            it.Conn.ID(),
				Schema:            table.Schema,
				Table:             table.Table,
				PrimaryKeyColumns: table.PrimaryKeyColumns,
				PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
			})
			stats.numExtraneous++
		}
	}

	reporter.Report(StatusReport{
		Info: fmt.Sprintf("finished row verification on %s.%s (shard %d/%d): %s", table.Schema, table.Table, table.ShardNum, table.TotalShards, stats.String()),
	})
	if TimingEnabled {
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		reporter.Report(StatusReport{
			Info: fmt.Sprintf("scanned %d rows in %s (%.2f rows/sec)", stats.numVerified, duration, float64(stats.numVerified)/(float64(duration)/float64(time.Second))),
		})
	}

	return nil
}
