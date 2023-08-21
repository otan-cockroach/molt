package dataexport

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/rowiterator"
)

type crdbSource struct {
	aost time.Time
	conn dbconn.Conn
}

func NewCRDBSource(ctx context.Context, conn *dbconn.PGConn) (*crdbSource, error) {
	return &crdbSource{
		conn: conn,
		aost: time.Now().UTC().Truncate(time.Second),
	}, nil
}

func (c *crdbSource) CDCCursor() string {
	return c.aost.Format(time.RFC3339Nano)
}

func (c *crdbSource) Conn(ctx context.Context) (SourceConn, error) {
	conn, err := c.conn.Clone(ctx)
	if err != nil {
		return nil, err
	}
	return &crdbSourceConn{conn: conn, src: c}, nil
}

func (c *crdbSource) Close(ctx context.Context) error {
	return nil
}

type crdbSourceConn struct {
	conn dbconn.Conn
	src  *crdbSource
}

func (c *crdbSourceConn) Export(
	ctx context.Context, writer io.Writer, table dbtable.VerifiedTable,
) error {
	return scanWithRowIterator(ctx, c.conn, writer, rowiterator.ScanTable{
		Table: rowiterator.Table{
			Name:              table.Name,
			ColumnNames:       table.Columns,
			ColumnOIDs:        table.ColumnOIDs[0],
			PrimaryKeyColumns: table.PrimaryKeyColumns,
		},
		AOST: &c.src.aost,
	})
}

func (c crdbSourceConn) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}
