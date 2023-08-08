package dataexport

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/rowiterator"
	"github.com/jackc/pgx/v5"
)

type Source interface {
	CDCCursor() string
	Conn(ctx context.Context) (SourceConn, error)
	Close(ctx context.Context) error
}

type SourceConn interface {
	Export(ctx context.Context, writer io.Writer, table dbtable.VerifiedTable) error
	Close(ctx context.Context) error
}

func InferExportSource(ctx context.Context, conn dbconn.Conn) (Source, error) {
	switch conn := conn.(type) {
	case *dbconn.PGConn:
		if conn.IsCockroach() {
			return &crdbSource{
				conn: conn,
				aost: time.Now().UTC().Truncate(time.Second),
			}, nil
		}
		// TODO: we should create a replication slot here.
		var cdcCursor string
		if err := conn.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&cdcCursor); err != nil {
			return nil, errors.Wrap(err, "failed to export wal LSN")
		}
		// Keep tx with snapshot open to establish a consistent snapshot.
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:   pgx.RepeatableRead,
			AccessMode: pgx.ReadOnly,
		})
		if err != nil {
			return nil, err
		}
		var snapshotID string
		if err := func() error {
			if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
				return errors.Wrap(err, "failed to export snapshot")
			}
			return nil
		}(); err != nil {
			return nil, errors.CombineErrors(err, tx.Rollback(ctx))
		}
		return &pgSource{
			snapshotID: snapshotID,
			cdcCursor:  cdcCursor,
			tx:         tx,
			conn:       conn,
		}, nil
	case *dbconn.MySQLConn:
		var source string
		var start, end int
		if err := func() error {
			if err := conn.QueryRowContext(ctx, "select source_uuid, min(interval_start), max(interval_end) from mysql.gtid_executed group by source_uuid").Scan(
				&source, &start, &end,
			); err != nil {
				return errors.Wrap(err, "failed to export snapshot")
			}
			return nil
		}(); err != nil {
			return nil, err
		}
		return &mysqlSource{
			gtid: fmt.Sprintf("%s:%d-%d", source, start, end),
			conn: conn,
		}, nil
	}
	return nil, errors.AssertionFailedf("unknown conn type: %T", conn)
}

func scanWithRowIterator(
	ctx context.Context, c dbconn.Conn, writer io.Writer, table rowiterator.ScanTable,
) error {
	cw := csv.NewWriter(writer)
	it, err := rowiterator.NewScanIterator(
		ctx,
		c,
		table,
		100_000,
	)
	if err != nil {
		return err
	}
	strings := make([]string, 0, len(table.ColumnNames))
	for it.HasNext(ctx) {
		strings = strings[:0]
		datums := it.Next(ctx)
		for _, d := range datums {
			f := tree.NewFmtCtx(tree.FmtBareStrings | tree.FmtParsableNumerics)
			f.FormatNode(d)
			strings = append(strings, f.CloseAndGetString())
		}
		if err := cw.Write(strings); err != nil {
			return err
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	cw.Flush()
	return nil
}
