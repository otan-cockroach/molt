package dataexport

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove/dataquery"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/jackc/pgx/v5"
)

type pgSource struct {
	tx         pgx.Tx
	conn       dbconn.Conn
	snapshotID string
	cdcCursor  string
}

func (p *pgSource) Close(ctx context.Context) error {
	return p.tx.Rollback(ctx)
}

func (p *pgSource) CDCCursor() string {
	return p.cdcCursor
}

func (p *pgSource) Conn(ctx context.Context) (SourceConn, error) {
	conn, err := p.conn.Clone(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.(*dbconn.PGConn).BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, errors.CombineErrors(err, conn.Close(ctx))
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", p.snapshotID)); err != nil {
		return nil, errors.CombineErrors(err, conn.Close(ctx))
	}
	return &pgSourceConn{
		conn: conn,
		tx:   tx,
	}, nil
}

type pgSourceConn struct {
	conn dbconn.Conn
	tx   pgx.Tx
}

func (p *pgSourceConn) Export(
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

func (p *pgSourceConn) Close(ctx context.Context) error {
	return p.conn.Close(ctx)
}
