package tableverify

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/mysqlconv"
	"github.com/lib/pq/oid"
)

type Column struct {
	Name    tree.Name
	OID     oid.Oid
	NotNull bool
}

func GetColumns(ctx context.Context, conn dbconn.Conn, table dbtable.DBTable) ([]Column, error) {
	var ret []Column

	switch conn := conn.(type) {
	case *dbconn.PGConn:
		rows, err := conn.Query(
			ctx,
			`SELECT attname, atttypid, attnotnull FROM pg_attribute WHERE attrelid = $1 AND attnum > 0`,
			table.OID,
		)
		if err != nil {
			return ret, err
		}

		for rows.Next() {
			var cm Column
			if err := rows.Scan(&cm.Name, &cm.OID, &cm.NotNull); err != nil {
				return ret, errors.Wrap(err, "error decoding column metadata")
			}
			ret = append(ret, cm)
		}
		if rows.Err() != nil {
			return ret, errors.Wrap(err, "error collecting column metadata")
		}
	case *dbconn.MySQLConn:
		rows, err := conn.QueryContext(
			ctx,
			`SELECT
column_name, data_type, column_type, is_nullable
FROM information_schema.columns
WHERE table_schema = database() AND table_name = ?
ORDER BY ordinal_position`,
			string(table.Table),
		)
		if err != nil {
			return ret, err
		}

		for rows.Next() {
			var cn string
			var ct string
			var dt string
			var isNullable string
			if err := rows.Scan(&cn, &dt, &ct, &isNullable); err != nil {
				return ret, errors.Wrap(err, "error decoding column metadata")
			}
			var cm Column
			cm.Name = tree.Name(strings.ToLower(cn))
			cm.OID = mysqlconv.DataTypeToOID(dt, ct)
			cm.NotNull = isNullable == "NO"
			ret = append(ret, cm)
		}
		if rows.Err() != nil {
			return ret, errors.Wrap(err, "error collecting column metadata")
		}
	default:
		return nil, errors.Newf("connection %T not supported", conn)
	}
	return ret, nil
}

func getColumnsForTables(
	ctx context.Context, conns dbconn.OrderedConns, tbls [2]dbtable.DBTable,
) ([2][]Column, error) {
	var ret [2][]Column
	for i, conn := range conns {
		var err error
		ret[i], err = GetColumns(ctx, conn, tbls[i])
		if err != nil {
			return ret, err
		}
	}
	return ret, nil
}

func getPrimaryKeysForTables(
	ctx context.Context, conns dbconn.OrderedConns, tbls [2]dbtable.DBTable,
) ([2][]tree.Name, error) {
	var ret [2][]tree.Name
	for i, conn := range conns {
		var err error
		ret[i], err = getPrimaryKey(ctx, conn, tbls[i])
		if err != nil {
			return ret, err
		}
	}
	return ret, nil
}

func getPrimaryKey(
	ctx context.Context, conn dbconn.Conn, table dbtable.DBTable,
) ([]tree.Name, error) {
	var ret []tree.Name

	switch conn := conn.(type) {
	case *dbconn.PGConn:
		rows, err := conn.Query(
			ctx,
			`
select
    a.attname as column_name
from
    pg_class t
    join pg_attribute a on a.attrelid = t.oid
    join pg_index ix    on t.oid = ix.indrelid AND a.attnum = ANY(ix.indkey)
    join pg_class i     on i.oid = ix.indexrelid  
where
    t.oid = $1 AND indisprimary;
`,
			table.OID,
		)
		if err != nil {
			return ret, err
		}

		for rows.Next() {
			var c tree.Name
			if err := rows.Scan(&c); err != nil {
				return ret, errors.Wrap(err, "error decoding column name")
			}
			ret = append(ret, c)
		}
		if rows.Err() != nil {
			return ret, errors.Wrap(err, "error collecting primary key")
		}
	case *dbconn.MySQLConn:
		rows, err := conn.QueryContext(
			ctx,
			`SELECT k.column_name
FROM information_schema.table_constraints t
JOIN information_schema.key_column_usage k
USING(constraint_name,table_schema,table_name)
WHERE t.constraint_type = 'PRIMARY KEY'
  AND t.table_schema = database()
  AND t.table_name = ?
  ORDER BY k.ordinal_position`,
			string(table.Table),
		)
		if err != nil {
			return ret, err
		}

		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				return ret, errors.Wrap(err, "error decoding column name")
			}
			ret = append(ret, tree.Name(strings.ToLower(c)))
		}
		if rows.Err() != nil {
			return ret, errors.Wrap(err, "error collecting primary key")
		}
	}
	return ret, nil
}

func mapColumns(cols []Column) map[tree.Name]Column {
	ret := make(map[tree.Name]Column, len(cols))
	for _, col := range cols {
		ret[col.Name] = col
	}
	return ret
}
