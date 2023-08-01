package datamove

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove/dataquery"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/rowiterator"
	"github.com/jackc/pgx/v5"
)

type ExportSource interface {
	CDCCursor() string
	Export(ctx context.Context, writer io.Writer, table dbtable.VerifiedTable) error
	Close(ctx context.Context) error
}

func InferExportSource(ctx context.Context, conn dbconn.Conn) (ExportSource, error) {
	switch conn := conn.(type) {
	case *dbconn.PGConn:
		if conn.IsCockroach() {
			return &crdbExportSource{
				conn: conn,
				aost: time.Now().UTC().Truncate(time.Second),
			}, nil
		}
		var cdcCursor string
		if err := conn.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&cdcCursor); err != nil {
			return nil, errors.Wrap(err, "failed to export wal LSN")
		}
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{
			IsoLevel: pgx.RepeatableRead,
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
		return &pgExportSource{
			snapshotID: snapshotID,
			cdcCursor:  cdcCursor,
			tx:         tx,
		}, nil
	case *dbconn.MySQLConn:
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
			ReadOnly:  true,
		})
		if err != nil {
			return nil, err
		}

		var source string
		var start, end int
		if err := func() error {
			if err := tx.QueryRowContext(ctx, "select source_uuid, min(interval_start), max(interval_end) from mysql.gtid_executed;").Scan(
				&source, &start, &end,
			); err != nil {
				return errors.Wrap(err, "failed to export snapshot")
			}
			return nil
		}(); err != nil {
			return nil, errors.CombineErrors(err, tx.Rollback())
		}
		return &mysqlExportSource{
			gtid: fmt.Sprintf("%s:%d-%d", source, start, end),
			tx:   tx,
			conn: conn,
		}, nil
	}
	return nil, errors.AssertionFailedf("unknown conn type: %T", conn)
}

type pgExportSource struct {
	tx         pgx.Tx
	snapshotID string
	cdcCursor  string
}

func (p *pgExportSource) Export(
	ctx context.Context, writer io.Writer, table dbtable.VerifiedTable,
) error {
	if _, err := p.tx.Conn().PgConn().CopyTo(
		ctx,
		writer,
		dataquery.NewPGCopyTo(table),
	); err != nil {
		return err
	}
	return nil
}

func (p *pgExportSource) Close(ctx context.Context) error {
	return p.tx.Rollback(ctx)
}

func (p *pgExportSource) CDCCursor() string {
	return p.cdcCursor
}

type mysqlExportSource struct {
	gtid string
	conn dbconn.Conn
	tx   *sql.Tx
}

func (m *mysqlExportSource) CDCCursor() string {
	return m.gtid
}

func (m *mysqlExportSource) Export(
	ctx context.Context, writer io.Writer, table dbtable.VerifiedTable,
) error {
	return scanWithRowIterator(ctx, m.conn, writer, rowiterator.ScanTable{
		Table: rowiterator.Table{
			Name:              table.Name,
			ColumnNames:       table.Columns,
			ColumnOIDs:        table.ColumnOIDs[0],
			PrimaryKeyColumns: table.PrimaryKeyColumns,
		},
	})
}

func (m *mysqlExportSource) Close(ctx context.Context) error {
	return m.tx.Rollback()
}

type crdbExportSource struct {
	aost time.Time
	conn dbconn.Conn
}

func (c *crdbExportSource) CDCCursor() string {
	return c.aost.Format(time.RFC3339Nano)
}

func (c *crdbExportSource) Export(
	ctx context.Context, writer io.Writer, table dbtable.VerifiedTable,
) error {
	return scanWithRowIterator(ctx, c.conn, writer, rowiterator.ScanTable{
		Table: rowiterator.Table{
			Name:              table.Name,
			ColumnNames:       table.Columns,
			ColumnOIDs:        table.ColumnOIDs[0],
			PrimaryKeyColumns: table.PrimaryKeyColumns,
		},
		AOST: &c.aost,
	})
}

func (c *crdbExportSource) Close(ctx context.Context) error {
	return nil
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
