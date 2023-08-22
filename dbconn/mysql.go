package dbconn

import (
	"context"
	"database/sql"

	"github.com/cockroachdb/molt/mysqlurl"
	"github.com/jackc/pgx/v5/pgtype"
)

type MySQLConn struct {
	id  ID
	url string
	*sql.DB
	typeMap *pgtype.Map
}

func ConnectMySQL(ctx context.Context, id ID, connStr string) (*MySQLConn, error) {
	cfg, err := mysqlurl.Parse(connStr)
	if err != nil {
		return nil, err
	}
	u := cfg.FormatDSN()
	db, err := sql.Open("mysql", u)
	if err != nil {
		return nil, err
	}
	m := pgtype.NewMap()
	return &MySQLConn{id: id, url: u, DB: db, typeMap: m}, nil
}

func (c *MySQLConn) ID() ID {
	return c.id
}

func (c *MySQLConn) Close(ctx context.Context) error {
	return c.DB.Close()
}

func (c *MySQLConn) Clone(ctx context.Context) (Conn, error) {
	ret, err := ConnectMySQL(ctx, c.id, c.url)
	if err != nil {
		return nil, err
	}
	ret.typeMap = c.typeMap
	return ret, nil
}

func (c *MySQLConn) TypeMap() *pgtype.Map {
	return c.typeMap
}

func (c *MySQLConn) IsCockroach() bool {
	return false
}

func (c *MySQLConn) ConnStr() string {
	return c.url
}

var _ Conn = (*MySQLConn)(nil)

func (c *MySQLConn) Dialect() string {
	return "MySQL"
}
