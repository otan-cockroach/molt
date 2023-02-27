package dbconn

import (
	"context"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/lexbase"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

type ID string

type Conn interface {
	ID() ID
	// Close closes the connection.
	Close(ctx context.Context) error
	// Clone creates a new Conn with the same underlying connections arguments.
	Clone(ctx context.Context) (Conn, error)
	// TypeMap returns a pgx typemap.
	// TODO: consider whether pgtype.Map is the right abstraction.
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
		cfg := MySQLURLToDSN(u)
		return ConnectMySQL(ctx, id, cfg)
	}
	return nil, errors.Newf("unrecognised scheme %s from %s", u.Scheme, connStr)
}

// TestOnlyCleanDatabase returns a connection to a clean database.
// This is recommended for test use only
func TestOnlyCleanDatabase(ctx context.Context, id ID, url string, dbName string) (Conn, error) {
	c, err := Connect(ctx, id, url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close(ctx) }()

	switch c := c.(type) {
	case *PGConn:
		if _, err := c.Exec(ctx, "DROP DATABASE IF EXISTS "+lexbase.EscapeSQLIdent(dbName)); err != nil {
			return nil, err
		}
		if _, err := c.Exec(ctx, "CREATE DATABASE "+lexbase.EscapeSQLIdent(dbName)); err != nil {
			return nil, err
		}
		cfgCopy := c.Config().Copy()
		cfgCopy.Database = dbName
		pgConn, err := pgx.ConnectConfig(ctx, cfgCopy)
		if err != nil {
			return nil, err
		}
		return NewPGConn(c.id, pgConn), nil
	case *MySQLConn:
		// TODO: escape
		if _, err := c.ExecContext(ctx, "DROP DATABASE IF EXISTS "+dbName); err != nil {
			return nil, err
		}
		// TODO: escape
		if _, err := c.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
			return nil, err
		}
		cfgCopy := c.cfg.Clone()
		cfgCopy.DBName = dbName
		return ConnectMySQL(ctx, c.id, cfgCopy)
	}
	return nil, errors.AssertionFailedf("clean database not supported for %T", c)
}
