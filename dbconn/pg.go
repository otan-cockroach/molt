package dbconn

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
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

func NewPGConn(id ID, conn *pgx.Conn, connStr string, version string) *PGConn {
	return &PGConn{
		id:          id,
		Conn:        conn,
		version:     version,
		connStr:     connStr,
		isCockroach: strings.Contains(version, "CockroachDB"),
	}
}

func (c *PGConn) ID() ID {
	return c.id
}

func (c *PGConn) IsCockroach() bool {
	return c.isCockroach
}

func (c *PGConn) SQLDriver() interface{} {
	return c.Conn
}

func (c *PGConn) Clone(ctx context.Context) (Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, c.Config())
	if err != nil {
		return nil, err
	}
	return NewPGConn(c.id, conn, c.connStr, c.version), nil
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
