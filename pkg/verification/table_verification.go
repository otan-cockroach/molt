package verification

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/dbconn"
	"github.com/cockroachdb/molt/pkg/mysqlconv"
	"github.com/lib/pq/oid"
)

type columnMetadata struct {
	columnName tree.Name
	// TODO: account for geospatial, enums, user defined types.
	// TODO: compare typMod, which CRDB does not really support.
	typeOID oid.Oid
	notNull bool
}

func getColumns(
	ctx context.Context, conn dbconn.Conn, table TableMetadata,
) ([]columnMetadata, error) {
	var ret []columnMetadata

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
			var cm columnMetadata
			if err := rows.Scan(&cm.columnName, &cm.typeOID, &cm.notNull); err != nil {
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
			`SELECT column_name, data_type, column_type, is_nullable FROM information_schema.columns
WHERE table_schema = database() AND table_name = ?`,
			string(table.Table),
		)
		if err != nil {
			return ret, err
		}

		for rows.Next() {
			var cm columnMetadata
			var ct string
			var dt string
			var isNullable string
			if err := rows.Scan(&cm.columnName, &dt, &ct, &isNullable); err != nil {
				return ret, errors.Wrap(err, "error decoding column metadata")
			}
			cm.typeOID = mysqlconv.DataTypeToOID(dt, ct)
			cm.notNull = isNullable == "NO"
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

func getPrimaryKey(
	ctx context.Context, conn dbconn.Conn, table TableMetadata,
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
  AND t.table_name= ?`,
			string(table.Table),
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
	}
	return ret, nil
}

func mapColumns(cols []columnMetadata) map[tree.Name]columnMetadata {
	ret := make(map[tree.Name]columnMetadata, len(cols))
	for _, col := range cols {
		ret[col.columnName] = col
	}
	return ret
}

type verifyTableResult struct {
	Schema                      tree.Name
	Table                       tree.Name
	RowVerifiable               bool
	MatchingColumns             []tree.Name
	ColumnTypeOIDs              [][]oid.Oid
	PrimaryKeyColumns           []tree.Name
	MismatchingTableDefinitions []MismatchingTableDefinition
}

func verifyCommonTables(
	ctx context.Context, conns []dbconn.Conn, tables map[dbconn.ID][]TableMetadata,
) ([]verifyTableResult, error) {
	var ret []verifyTableResult

	columnOIDMap := make(map[int]map[tree.Name]oid.Oid)

	truthConn := conns[0]
	for tblIdx, truthTbl := range tables[truthConn.ID()] {
		res := verifyTableResult{
			Schema: truthTbl.Schema,
			Table:  truthTbl.Table,
		}
		truthCols, err := getColumns(ctx, truthConn, truthTbl)
		if err != nil {
			return nil, errors.Wrap(err, "error getting columns for table")
		}
		columnOIDMap[0] = make(map[tree.Name]oid.Oid)
		for _, truthCol := range truthCols {
			columnOIDMap[0][truthCol.columnName] = truthCol.typeOID
		}

		pkSame := true
		truthPKCols, err := getPrimaryKey(ctx, truthConn, truthTbl)
		if err != nil {
			return nil, errors.Wrap(err, "error getting primary key")
		}
		if len(truthPKCols) == 0 {
			res.MismatchingTableDefinitions = append(
				res.MismatchingTableDefinitions,
				MismatchingTableDefinition{
					ConnID:        truthConn.ID(),
					TableMetadata: truthTbl,
					Info:          "missing a PRIMARY KEY - results cannot be compared",
				},
			)
		}
		comparableColumns := mapColumns(truthCols)

		for i := 1; i < len(conns); i++ {
			compareConn := conns[i]
			truthMappedCols := mapColumns(truthCols)
			targetTbl := tables[compareConn.ID()][tblIdx]
			compareColumns, err := getColumns(ctx, conns[i], targetTbl)
			if err != nil {
				return nil, errors.Wrap(err, "error getting columns for table")
			}
			columnOIDMap[i] = make(map[tree.Name]oid.Oid)
			for _, targetCol := range compareColumns {
				columnOIDMap[i][targetCol.columnName] = targetCol.typeOID
				sourceCol, ok := truthMappedCols[targetCol.columnName]
				if !ok {
					res.MismatchingTableDefinitions = append(
						res.MismatchingTableDefinitions,
						MismatchingTableDefinition{
							ConnID:        compareConn.ID(),
							TableMetadata: targetTbl,
							Info:          fmt.Sprintf("extraneous column %s found", targetCol.columnName),
						},
					)
					continue
				}

				delete(truthMappedCols, targetCol.columnName)

				if sourceCol.notNull != targetCol.notNull {
					res.MismatchingTableDefinitions = append(
						res.MismatchingTableDefinitions,
						MismatchingTableDefinition{
							ConnID:        compareConn.ID(),
							TableMetadata: targetTbl,
							Info: fmt.Sprintf(
								"column %s NOT NULL mismatch: %s=%t vs %s=%t",
								targetCol.columnName,
								truthConn.ID(),
								sourceCol.notNull,
								compareConn.ID(),
								targetCol.notNull,
							),
						},
					)
				}
				truthTyp, err := dbconn.GetDataType(ctx, truthConn, sourceCol.typeOID)
				if err != nil {
					return nil, err
				}
				compareTyp, err := dbconn.GetDataType(ctx, compareConn, targetCol.typeOID)
				if err != nil {
					return nil, err
				}
				if truthTyp.Name != compareTyp.Name {
					// TODO(otan): allow similar types to be compared anyway, e.g. int/int, float/float, json/jsonb.
					res.MismatchingTableDefinitions = append(
						res.MismatchingTableDefinitions,
						MismatchingTableDefinition{
							ConnID:        compareConn.ID(),
							TableMetadata: targetTbl,
							Info: fmt.Sprintf(
								"column type mismatch on %s: %s=%s vs %s=%s",
								targetCol.columnName,
								truthConn.ID(),
								truthTyp.Name,
								compareConn.ID(),
								compareTyp.Name,
							),
						},
					)
					delete(comparableColumns, sourceCol.columnName)
				}
			}
			for colName := range truthMappedCols {
				res.MismatchingTableDefinitions = append(
					res.MismatchingTableDefinitions,
					MismatchingTableDefinition{
						ConnID:        compareConn.ID(),
						TableMetadata: targetTbl,
						Info: fmt.Sprintf(
							"missing column %s",
							colName,
						),
					},
				)
				delete(comparableColumns, colName)
			}

			targetPKCols, err := getPrimaryKey(ctx, compareConn, targetTbl)
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
					if _, ok := comparableColumns[targetPKCols[i]]; !ok {
						currPKSame = false
						break
					}
				}
			}
			if !currPKSame && len(truthPKCols) > 0 {
				pkSame = false
				res.MismatchingTableDefinitions = append(
					res.MismatchingTableDefinitions,
					MismatchingTableDefinition{
						ConnID:        compareConn.ID(),
						TableMetadata: targetTbl,
						Info:          "PRIMARY KEY does not match source of truth (columns and types must match)",
					},
				)
			}
		}

		res.PrimaryKeyColumns = truthPKCols
		res.ColumnTypeOIDs = make([][]oid.Oid, len(conns))
		// Place PK columns first.
		for _, col := range truthPKCols {
			if _, ok := comparableColumns[col]; !ok {
				res.MatchingColumns = append(res.MatchingColumns, col)
				for i := range conns {
					res.ColumnTypeOIDs[i] = append(res.ColumnTypeOIDs[i], columnOIDMap[i][col])
				}
				delete(comparableColumns, col)
			}
		}
		// Then every other row.
		for _, col := range truthCols {
			if _, ok := comparableColumns[col.columnName]; ok {
				res.MatchingColumns = append(res.MatchingColumns, col.columnName)
				for i := range conns {
					res.ColumnTypeOIDs[i] = append(res.ColumnTypeOIDs[i], columnOIDMap[i][col.columnName])
				}
			}
		}
		res.RowVerifiable = pkSame && len(truthPKCols) > 0
		ret = append(ret, res)
	}
	return ret, nil
}
