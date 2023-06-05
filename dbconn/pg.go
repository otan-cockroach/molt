package dbconn

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type PGConn struct {
	id ID
	*pgx.Conn
}

var _ Conn = (*PGConn)(nil)

func NewPGConn(id ID, conn *pgx.Conn) *PGConn {
	return &PGConn{id: id, Conn: conn}
}

func (c *PGConn) ID() ID {
	return c.id
}

func (c *PGConn) SQLDriver() interface{} {
	return c.Conn
}

func (c *PGConn) Clone(ctx context.Context) (Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, c.Config())
	if err != nil {
		return nil, err
	}
	return NewPGConn(c.id, conn), nil
}
