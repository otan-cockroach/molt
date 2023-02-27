package dbconn

import (
	"context"
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type ID string

type Conn interface {
	ID() ID
	// Close closes the connection.
	Close(ctx context.Context) error
	// Clone creates a new Conn with the same underlying connections arguments.
	Clone(ctx context.Context) (Conn, error)
	// TypeMap returns a pgx typemap.
	// TODO: consider whether this is the right abstraction.
	TypeMap() *pgtype.Map
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
		id = ID(u.Hostname() + ":" + u.Port())
	}

	switch {
	case strings.Contains(u.Scheme, "postgres"):
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			return nil, errors.Wrapf(err, "error connecting to %s", connStr)
		}
		return NewPGConn(id, conn), nil
	case strings.Contains(u.Scheme, "mysql"):
		return ConnectMySQL(ctx, id, u)
	}
	return nil, errors.Newf("unrecognised scheme %s from %s", u.Scheme, connStr)
}
