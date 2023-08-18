package dbconn

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq/oid"
)

type PGConn struct {
	id ID
	*pgx.Conn
	version     string
	connStr     string
	isCockroach bool
}

var _ Conn = (*PGConn)(nil)

func ConnectPG(ctx context.Context, id ID, connStr string) (*PGConn, error) {
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	return ConnectPGConfig(ctx, id, cfg)
}

func ConnectPGConfig(ctx context.Context, id ID, cfg *pgx.ConnConfig) (*PGConn, error) {
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "error connect")
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return nil, err
	}
	return &PGConn{
		id:          id,
		Conn:        conn,
		version:     version,
		connStr:     cfg.ConnString(),
		isCockroach: strings.Contains(version, "CockroachDB"),
	}, nil
}

func (c *PGConn) ID() ID {
	return c.id
}

func (c *PGConn) IsCockroach() bool {
	return c.isCockroach
}

func (c *PGConn) Clone(ctx context.Context) (Conn, error) {
	return ConnectPGConfig(ctx, c.id, c.Conn.Config())
}

func (c *PGConn) ConnStr() string {
	return c.connStr
}

func init() {
	// Inject JSON as a OidToType.
	types.OidToType[oid.T_json] = types.Jsonb
	types.OidToType[oid.T__json] = types.MakeArray(types.Jsonb)
}

func (c *PGConn) Dialect() string {
	if c.IsCockroach() {
		return "CockroachDB"
	}
	return "PostgreSQL"
}
