package dbconn

import (
	"context"
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

type ID string

type Conn interface {
	ID() ID
	// Close closes the connection.
	Close(ctx context.Context) error
	// Clone creates a new Conn with the same underlying connections arguments.
	Clone(ctx context.Context) (Conn, error)
}

func Connect(ctx context.Context, preferredID ID, connStr string) (Conn, error) {
	id := preferredID
	if len(connStr) == 0 {
		return nil, errors.Newf("empty connection string")
	}
	u, err := url.Parse(connStr)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse url: %s", connStr)
	}

	if id == "" {
		id = ID(u.Hostname())
	}

	switch {
	case strings.Contains(u.Scheme, "postgres"):
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			return nil, errors.Wrapf(err, "error connecting to %s", connStr)
		}
		return NewPGConn(id, conn), nil
	}
	return nil, errors.Newf("unrecognised scheme %s from %s", u.Scheme, connStr)
}

type PGConn struct {
	id ID
	*pgx.Conn
}

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

var _ Conn = (*PGConn)(nil)
