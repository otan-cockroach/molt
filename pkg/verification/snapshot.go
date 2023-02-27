package verification

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/dbconn"
	"github.com/cockroachdb/molt/pkg/rowiterator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const DefaultWriteBatchSize = 10000

type snapshotOpts struct {
	writeBatchSize int
}

type SnapshotOpt func(opts *snapshotOpts)

func WithWriteBatchSize(c int) SnapshotOpt {
	return func(o *snapshotOpts) {
		o.writeBatchSize = c
	}
}

func Snapshot(inOpts ...SnapshotOpt) WorkFunc {
	opts := snapshotOpts{
		writeBatchSize: DefaultWriteBatchSize,
	}
	for _, applyOpt := range inOpts {
		applyOpt(&opts)
	}
	return func(
		ctx context.Context, conns []dbconn.Conn, table TableShard, rowBatchSize int, reporter Reporter,
	) error {
		truthConn := conns[0]
		it, err := rowiterator.NewIterator(
			ctx,
			truthConn,
			rowiterator.Table{
				Schema:            table.Schema,
				Table:             table.Table,
				ColumnNames:       table.MatchingColumns,
				ColumnOIDs:        table.MatchingColumnTypeOIDs[0],
				PrimaryKeyColumns: table.PrimaryKeyColumns,
				StartPKVals:       table.StartPKVals,
				EndPKVals:         table.EndPKVals,
			},
			rowBatchSize,
		)
		if err != nil {
			return errors.Wrapf(err, "error initializing row iterator on %s", truthConn.ID())
		}

		var cols []string
		for _, col := range table.MatchingColumns {
			cols = append(cols, string(col))
		}

		numAdded := 0
		rows := make([][]any, 0, opts.writeBatchSize)
		flush := func() error {
			if len(rows) == 0 {
				return nil
			}
			for _, conn := range conns[1:] {
				switch conn := conn.(type) {
				case *dbconn.PGConn:
					for {
						_, err := conn.CopyFrom(
							ctx,
							pgx.Identifier{string(table.Schema), string(table.Table)},
							cols,
							pgx.CopyFromRows(rows),
						)
						if err == nil {
							break

						}
						if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
							if pgErr.Code == "40001" {
								reporter.Report(StatusReport{
									Info: fmt.Sprintf("retry on %s.%s (shard %d/%d): %s", table.Schema, table.Table, table.ShardNum, table.TotalShards, err.Error()),
								})
							}
							return err
						}
						return err
					}
				default:
					return errors.AssertionFailedf("connection type %T not supported", conn)
				}
			}
			numAdded += len(rows)
			rows = rows[:0]
			return nil
		}
		numSeen := 0
		for it.HasNext(ctx) {
			if numSeen%10000 == 0 && numSeen > 0 {
				reporter.Report(StatusReport{
					Info: fmt.Sprintf("progress on %s.%s (shard %d/%d): %d rows seen", table.Schema, table.Table, table.ShardNum, table.TotalShards, numSeen),
				})
			}
			numSeen++
			vals := it.Next(ctx)
			toAppend := make([]any, len(table.MatchingColumns))
			// For now, always include the string value.
			for i, val := range vals {
				f := tree.NewFmtCtx(tree.FmtBareStrings | tree.FmtParsableNumerics)
				f.FormatNode(val)
				toAppend[i] = f.CloseAndGetString()
			}
			rows = append(rows, toAppend)

			if len(rows) == opts.writeBatchSize {
				if err := flush(); err != nil {
					return errors.Wrapf(err, "error flushing results")
				}
			}
		}
		if it.Error() != nil {
			return errors.Wrapf(err, "error during iteration")
		}
		if err := flush(); err != nil {
			return errors.Wrapf(err, "error flushing results")
		}
		reporter.Report(StatusReport{
			Info: fmt.Sprintf("finished snapshot on %s.%s (shard %d/%d): %d rows", table.Schema, table.Table, table.ShardNum, table.TotalShards, numAdded),
		})

		return nil
	}
}
