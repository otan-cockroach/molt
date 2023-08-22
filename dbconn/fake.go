package dbconn

import (
	"context"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/jackc/pgx/v5/pgtype"
)

type FakeConn struct {
	id     ID
	typMap *pgtype.Map
}

func MakeFakeConn(id ID) FakeConn {
	return FakeConn{id: id, typMap: pgtype.NewMap()}
}

func (f FakeConn) ID() ID {
	return f.id
}

func (f FakeConn) Close(ctx context.Context) error {
	return nil
}

func (f FakeConn) Clone(ctx context.Context) (Conn, error) {
	return f, nil
}

func (f FakeConn) TypeMap() *pgtype.Map {
	return f.typMap
}

func (f FakeConn) Database() tree.Name {
	return tree.Name(f.id)
}

func (f FakeConn) IsCockroach() bool {
	return false
}

func (f FakeConn) ConnStr() string {
	return "fake://"
}

func (f FakeConn) Dialect() string {
	return "fake"
}
