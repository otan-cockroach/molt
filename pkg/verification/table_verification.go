package verification

import (
	"context"
	"log"

	"github.com/cockroachdb/errors"
)

type columnName string

type columnMetadata struct {
	columnName columnName
	// TODO: account for geospatial types.
	// TODO: compare typMod, which CRDB does not really support.
	typeOID OID
	notNull bool
}

type comparableTable struct {
	schema string
	name   string
	cols   []columnName
	pk     []columnName
}

func getColumns(ctx context.Context, conn Conn, tableOID OID) ([]columnMetadata, error) {
	var ret []columnMetadata

	rows, err := conn.Conn.Query(
		ctx,
		`SELECT attname, atttypid, attnotnull FROM pg_attribute WHERE attrelid = $1 AND attnum > 0`,
		tableOID,
	)
	if err != nil {
		return ret, err
	}

	for rows.Next() {
		var cm columnMetadata
		if err := rows.Scan(&cm.columnName, &cm.typeOID, &cm.notNull); err != nil {
			return ret, errors.Wrap(err, "error decoding column metadata")
		}
		ret = append(ret, cm)
	}
	if rows.Err() != nil {
		return ret, errors.Wrap(err, "error collecting column metadata")
	}
	return ret, nil
}

func getPrimaryKey(ctx context.Context, conn Conn, tableOID OID) ([]columnName, error) {
	var ret []columnName

	rows, err := conn.Conn.Query(
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
		tableOID,
	)
	if err != nil {
		return ret, err
	}

	for rows.Next() {
		var c columnName
		if err := rows.Scan(&c); err != nil {
			return ret, errors.Wrap(err, "error decoding column name")
		}
		ret = append(ret, c)
	}
	if rows.Err() != nil {
		return ret, errors.Wrap(err, "error collecting primary key")
	}
	return ret, nil
}

func mapColumns(cols []columnMetadata) map[columnName]columnMetadata {
	ret := make(map[columnName]columnMetadata, len(cols))
	for _, col := range cols {
		ret[col.columnName] = col
	}
	return ret
}

func verifyCommonTables(
	ctx context.Context, conns []Conn, tables map[ConnID][]tableMetadata,
) ([]comparableTable, error) {
	var ret []comparableTable
	for tblIdx, truthTbl := range tables[conns[0].ID] {
		truthCols, err := getColumns(ctx, conns[0], truthTbl.OID)
		if err != nil {
			return nil, errors.Wrap(err, "error getting columns for table")
		}

		pkSame := true
		truthPKCols, err := getPrimaryKey(ctx, conns[0], truthTbl.OID)
		if err != nil {
			return nil, errors.Wrap(err, "error getting primary key")
		}
		if len(truthPKCols) == 0 {
			log.Printf("[%s] table %s.%s has no primary key", conns[0].ID, truthTbl.Schema, truthTbl.Table)
			pkSame = false
		}
		comparableColumns := mapColumns(truthCols)

		for i := 1; i < len(conns); i++ {
			conn := conns[i]
			truthMappedCols := mapColumns(truthCols)
			targetTbl := tables[conn.ID][tblIdx]
			compareColumns, err := getColumns(ctx, conns[i], targetTbl.OID)
			if err != nil {
				return nil, errors.Wrap(err, "error getting columns for table")
			}
			for _, targetCol := range compareColumns {
				sourceCol, ok := truthMappedCols[targetCol.columnName]
				if !ok {
					log.Printf("[%s] table %s.%s has extra column %s", conn.ID, truthTbl.Schema, truthTbl.Table, targetCol.columnName)
					continue
				}
				if sourceCol.notNull != targetCol.notNull {
					log.Printf("[%s] warning! table %s.%s column %s nullability does not match", conn.ID, truthTbl.Schema, truthTbl.Table, sourceCol.columnName)
				}
				if sourceCol.typeOID != targetCol.typeOID {
					log.Printf("[%s] warning! table %s.%s column %s types do not match; cannot compare", conn.ID, truthTbl.Schema, truthTbl.Table, sourceCol.columnName)
					delete(comparableColumns, sourceCol.columnName)
				}
				delete(truthMappedCols, targetCol.columnName)
			}
			for colName := range truthMappedCols {
				log.Printf("[%s] table %s.%s has missing column %s", conn.ID, truthTbl.Schema, truthTbl.Table, colName)
				delete(comparableColumns, colName)
			}

			targetPKCols, err := getPrimaryKey(ctx, conn, targetTbl.OID)
			if err != nil {
				return nil, errors.Wrap(err, "error getting primary key")
			}

			currPKSame := len(targetPKCols) == len(truthPKCols)
			if currPKSame {
				for i := range targetPKCols {
					if targetPKCols[i] != truthPKCols[i] {
						currPKSame = false
						break
					}
				}
			}
			if !currPKSame {
				pkSame = false
				log.Printf("[%s] table %s.%s has a mismatching PK", conn.ID, truthTbl.Schema, truthTbl.Table)
			}
		}

		if pkSame {
			ct := comparableTable{
				schema: truthTbl.Schema,
				name:   truthTbl.Table,
				pk:     truthPKCols,
			}
			for _, col := range truthPKCols {
				if _, ok := comparableColumns[col]; !ok {
					ct.cols = append(ct.cols, col)
					delete(comparableColumns, col)
				}
			}
			for _, col := range truthCols {
				if _, ok := comparableColumns[col.columnName]; ok {
					ct.cols = append(ct.cols, col.columnName)
				}
			}
			if len(ct.cols) > 0 {
				ret = append(ret, ct)
			}
		}
	}
	return ret, nil
}
