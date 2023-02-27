package dbconn

import (
	"context"
	"database/sql"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgtype"
)

type MySQLConn struct {
	id ID
	u  *url.URL
	*sql.DB
	typeMap *pgtype.Map
}

func ConnectMySQL(ctx context.Context, id ID, u *url.URL) (*MySQLConn, error) {
	db, err := sql.Open("mysql", mysqlURLToDSN(u))
	if err != nil {
		return nil, err
	}
	return &MySQLConn{id: id, u: u, DB: db, typeMap: pgtype.NewMap()}, nil
}

func mysqlURLToDSN(u *url.URL) string {
	// TODO: TLS and the like
	dsn := &mysql.Config{
		Addr:   u.Host,
		Net:    "tcp",
		User:   "root",
		DBName: strings.TrimLeft(u.Path, "/"),
	}
	if uname := u.User; uname != nil {
		dsn.User = uname.Username()
		if p, ok := uname.Password(); ok {
			dsn.Passwd = p
		}
	}
	return dsn.FormatDSN()
}

func (c *MySQLConn) ID() ID {
	return c.id
}

func (c *MySQLConn) Close(ctx context.Context) error {
	return c.DB.Close()
}

func (c *MySQLConn) Clone(ctx context.Context) (Conn, error) {
	ret, err := ConnectMySQL(ctx, c.id, c.u)
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
