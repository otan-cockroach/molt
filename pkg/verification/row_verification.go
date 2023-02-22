package verification

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
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

type rowVerifiableTableShard struct {
	Schema                 tree.Name
	Table                  tree.Name
	MatchingColumns        []tree.Name
	MatchingColumnTypeOIDs [][]oid.Oid
	PrimaryKeyColumns      []tree.Name
	StartPKVals            []tree.Datum
	EndPKVals              []tree.Datum

	ShardNum    int
	TotalShards int
}

func compareRows(
	ctx context.Context,
	conns []Conn,
	table rowVerifiableTableShard,
	rowBatchSize int,
	reporter Reporter,
) error {
	iterators := make([]*rowIterator, len(conns))
	for i, conn := range conns {
		var err error
		iterators[i], err = newRowIterator(ctx, conn, i, table, rowBatchSize)
		if err != nil {
			return errors.Wrapf(err, "error initializing row iterator on %s", conn.Conn)
		}
	}

	cmpCtx := &compareContext{}

	var stats rowStats
	truth := iterators[0]
	for truth.hasNext(ctx) {
		if stats.numVerified%10000 == 0 && stats.numVerified > 0 {
			reporter.Report(StatusReport{
				Info: fmt.Sprintf("progress on %s.%s (shard %d/%d): %s", table.Schema, table.Table, table.ShardNum, table.TotalShards, stats.String()),
			})
		}
		stats.numVerified++

		truthVals := truth.next(ctx)
		for itIdx := 1; itIdx < len(iterators); itIdx++ {
			it := iterators[itIdx]

		itLoop:
			for {
				if !it.hasNext(ctx) && it.error() == nil {
					stats.numMissing++
					reporter.Report(MissingRow{
						ConnID:            it.conn.ID,
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  truthVals[:len(table.PrimaryKeyColumns)],
					})
					break
				}

				// Check the primary key.
				targetVals := it.peek(ctx)
				var compareVal int
				for i := range table.PrimaryKeyColumns {
					if compareVal = truthVals[i].Compare(cmpCtx, targetVals[i]); compareVal != 0 {
						break
					}
				}
				switch compareVal {
				case 1:
					// Extraneous row. Log and continue.
					it.next(ctx)
					reporter.Report(ExtraneousRow{
						ConnID:            it.conn.ID,
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
					})
					stats.numExtraneous++
				case 0:
					// Matching primary key. compare values and break loop.
					targetVals = it.next(ctx)
					mismatches := MismatchingRow{
						ConnID:            it.conn.ID,
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
						ConnID:            it.conn.ID,
						Schema:            table.Schema,
						Table:             table.Table,
						PrimaryKeyColumns: table.PrimaryKeyColumns,
						PrimaryKeyValues:  truthVals[:len(table.PrimaryKeyColumns)],
					})
					stats.numMissing++
					break itLoop
				}
			}
		}
	}

	for _, it := range iterators {
		for it.hasNext(ctx) {
			targetVals := it.next(ctx)
			reporter.Report(ExtraneousRow{
				ConnID:            it.conn.ID,
				Schema:            table.Schema,
				Table:             table.Table,
				PrimaryKeyColumns: table.PrimaryKeyColumns,
				PrimaryKeyValues:  targetVals[:len(table.PrimaryKeyColumns)],
			})
			stats.numExtraneous++
		}
		if err := it.error(); err != nil {
			reporter.Report(StatusReport{
				Info: fmt.Sprintf("error validating %s.%s on %s: %v", table.Schema, table.Table, it.conn.ID, err),
			})
		}
	}
	reporter.Report(StatusReport{
		Info: fmt.Sprintf("finished row verification on %s.%s (shard %d/%d): %s", table.Schema, table.Table, table.ShardNum, table.TotalShards, stats.String()),
	})

	return nil
}
