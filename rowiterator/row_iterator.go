package rowiterator

import (
	"context"
	"database/sql"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/mysqlconv"
	"github.com/cockroachdb/molt/pgconv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
)

type Iterator interface {
	Conn() dbconn.Conn
	HasNext(ctx context.Context) bool
	Error() error
	Peek(ctx context.Context) tree.Datums
	Next(ctx context.Context) tree.Datums
}

type Table struct {
	dbtable.Name
	ColumnNames       []tree.Name
	ColumnOIDs        []oid.Oid
	PrimaryKeyColumns []tree.Name
}

type ScanTable struct {
	Table
	AOST        *time.Time
	StartPKVals []tree.Datum
	EndPKVals   []tree.Datum
}

type rows interface {
	Err() error
	Next() bool
	Datums() (tree.Datums, error)
	Close()
}

type mysqlRows struct {
	*sql.Rows
	typMap  *pgtype.Map
	typOIDs []oid.Oid
}

func (r *mysqlRows) Datums() (tree.Datums, error) {
	return mysqlconv.ScanRowDynamicTypes(r.Rows, r.typMap, r.typOIDs)
}

func (r *mysqlRows) Close() {
	_ = r.Rows.Close()
}

type pgRows struct {
	pgx.Rows
	typMap  *pgtype.Map
	typOIDs []oid.Oid
}

func (r *pgRows) Datums() (tree.Datums, error) {
	vals, err := r.Values()
	if err != nil {
		return nil, err
	}
	return pgconv.ConvertRowValues(r.typMap, vals, r.typOIDs)
}
