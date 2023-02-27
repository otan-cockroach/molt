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
	id  ID
	cfg *mysql.Config
	*sql.DB
	typeMap *pgtype.Map
}

func ConnectMySQL(ctx context.Context, id ID, cfg *mysql.Config) (*MySQLConn, error) {
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	return &MySQLConn{id: id, cfg: cfg, DB: db, typeMap: pgtype.NewMap()}, nil
}

func MySQLURLToDSN(u *url.URL) *mysql.Config {
	// TODO: TLS and the like
	dsn := mysql.NewConfig()
	dsn.Addr = u.Host
	dsn.Net = "tcp"
	dsn.User = "root"
	dsn.DBName = strings.TrimLeft(u.Path, "/")
	if uname := u.User; uname != nil {
		dsn.User = uname.Username()
		if p, ok := uname.Password(); ok {
			dsn.Passwd = p
		}
	}
	return dsn
}

func (c *MySQLConn) ID() ID {
	return c.id
}

func (c *MySQLConn) Close(ctx context.Context) error {
	return c.DB.Close()
}

func (c *MySQLConn) Clone(ctx context.Context) (Conn, error) {
	ret, err := ConnectMySQL(ctx, c.id, c.cfg)
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
