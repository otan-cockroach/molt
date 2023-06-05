package dbconn

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5/pgtype"
)

type MySQLConn struct {
	id  ID
	url string
	*sql.DB
	typeMap *pgtype.Map
}

func ConnectMySQL(ctx context.Context, id ID, url string) (*MySQLConn, error) {
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	return &MySQLConn{id: id, url: url, DB: db, typeMap: pgtype.NewMap()}, nil
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

var _ Conn = (*MySQLConn)(nil)
