package dbconn

import (
	"context"
	"database/sql"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
)

type MySQLConn struct {
	id ID
	u  *url.URL
	*sql.DB
}

func ConnectMySQL(ctx context.Context, id ID, u *url.URL) (*MySQLConn, error) {
	dsn := &mysql.Config{
		Addr:   u.Host,
		User:   "root",
		DBName: strings.TrimLeft(u.Path, "/"),
	}
	if uname := u.User; uname != nil {
		dsn.User = uname.Username()
		if p, ok := uname.Password(); ok {
			dsn.Passwd = p
		}
	}
	// TODO: TLS and the like
	db, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return nil, err
	}
	return &MySQLConn{id: id, u: u, DB: db}, nil
}

func (c *MySQLConn) ID() ID {
	return c.id
}

func (c *MySQLConn) Close(ctx context.Context) error {
	return c.DB.Close()
}

func (c *MySQLConn) Clone(ctx context.Context) (Conn, error) {
	return ConnectMySQL(ctx, c.id, c.u)
}

var _ Conn = (*MySQLConn)(nil)
