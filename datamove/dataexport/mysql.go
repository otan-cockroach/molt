package dataexport

import (
	"context"
	"database/sql"
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
