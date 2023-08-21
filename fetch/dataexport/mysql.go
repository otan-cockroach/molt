package dataexport

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/rowiterator"
)

type mysqlSource struct {
	gtid string
	conn dbconn.Conn
}

func NewMySQLSource(ctx context.Context, conn *dbconn.MySQLConn) (*mysqlSource, error) {
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

func (m *mysqlSource) CDCCursor() string {
	return m.gtid
}

func (m *mysqlSource) Close(ctx context.Context) error {
	return nil
}

func (m *mysqlSource) Conn(ctx context.Context) (SourceConn, error) {
	conn, err := m.conn.Clone(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.(*dbconn.MySQLConn).BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, errors.CombineErrors(err, conn.Close(ctx))
	}
	return &mysqlConn{
		conn: conn,
		tx:   tx,
	}, nil
}

type mysqlConn struct {
	conn dbconn.Conn
	tx   *sql.Tx
}

func (m *mysqlConn) Export(
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

func (m *mysqlConn) Close(ctx context.Context) error {
	return m.conn.Close(ctx)
}
