package verification

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
)

type columnMetadata struct {
	columnName tree.Name
	// TODO: account for geospatial, enums, user defined types.
	// TODO: compare typMod, which CRDB does not really support.
	typeOID oid.Oid
	notNull bool
}

func getColumns(ctx context.Context, conn Conn, tableOID oid.Oid) ([]columnMetadata, error) {
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

func getPrimaryKey(ctx context.Context, conn Conn, tableOID oid.Oid) ([]tree.Name, error) {
	var ret []tree.Name

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
		var c tree.Name
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
	ctx context.Context, conns []Conn, tables map[ConnID][]TableMetadata,
) ([]verifyTableResult, error) {
	var ret []verifyTableResult

	columnOIDMap := make(map[int]map[tree.Name]oid.Oid)

	for tblIdx, truthTbl := range tables[conns[0].ID] {
		res := verifyTableResult{
			Schema: truthTbl.Schema,
			Table:  truthTbl.Table,
		}
		truthCols, err := getColumns(ctx, conns[0], truthTbl.OID)
		if err != nil {
			return nil, errors.Wrap(err, "error getting columns for table")
		}
		columnOIDMap[0] = make(map[tree.Name]oid.Oid)
		for _, truthCol := range truthCols {
			columnOIDMap[0][truthCol.columnName] = truthCol.typeOID
		}

		pkSame := true
		truthPKCols, err := getPrimaryKey(ctx, conns[0], truthTbl.OID)
		if err != nil {
			return nil, errors.Wrap(err, "error getting primary key")
		}
		if len(truthPKCols) == 0 {
			res.MismatchingTableDefinitions = append(
				res.MismatchingTableDefinitions,
				MismatchingTableDefinition{
					ConnID:        conns[0].ID,
					TableMetadata: truthTbl,
					Info:          "missing a PRIMARY KEY - results cannot be compared",
				},
			)
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
			columnOIDMap[i] = make(map[tree.Name]oid.Oid)
			for _, targetCol := range compareColumns {
				columnOIDMap[i][targetCol.columnName] = targetCol.typeOID
				sourceCol, ok := truthMappedCols[targetCol.columnName]
				if !ok {
					res.MismatchingTableDefinitions = append(
						res.MismatchingTableDefinitions,
						MismatchingTableDefinition{
							ConnID:        conn.ID,
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
							ConnID:        conn.ID,
							TableMetadata: targetTbl,
							Info: fmt.Sprintf(
								"column %s NOT NULL mismatch: %s=%t vs %s=%t",
								targetCol.columnName,
								conns[0].ID,
								sourceCol.notNull,
								conn.ID,
								targetCol.notNull,
							),
						},
					)
				}
				truthTyp, err := getDataType(ctx, conns[0].Conn, sourceCol.typeOID)
				if err != nil {
					return nil, err
				}
				compareTyp, err := getDataType(ctx, conn.Conn, targetCol.typeOID)
				if err != nil {
					return nil, err
				}
				if truthTyp.Name != compareTyp.Name {
					// TODO(otan): allow similar types to be compared anyway, e.g. int/int, float/float, json/jsonb.
					res.MismatchingTableDefinitions = append(
						res.MismatchingTableDefinitions,
						MismatchingTableDefinition{
							ConnID:        conn.ID,
							TableMetadata: targetTbl,
							Info: fmt.Sprintf(
								"column type mismatch on %s: %s=%s vs %s=%s",
								targetCol.columnName,
								conns[0].ID,
								truthTyp.Name,
								conn.ID,
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
						ConnID:        conn.ID,
						TableMetadata: targetTbl,
						Info: fmt.Sprintf(
							"missing column %s",
							colName,
						),
					},
				)
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
						ConnID:        conn.ID,
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

func getDataType(ctx context.Context, conn *pgx.Conn, oid oid.Oid) (*pgtype.Type, error) {
	if typ, ok := conn.TypeMap().TypeForOID(uint32(oid)); ok {
		return typ, nil
	}
	var typName string
	if err := conn.QueryRow(ctx, "SELECT $1::oid::regtype", oid).Scan(&typName); err != nil {
		return nil, errors.Wrapf(err, "error getting data type info for oid %d", oid)
	}
	typ, err := conn.LoadType(ctx, typName)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading type %s", typName)
	}
	conn.TypeMap().RegisterType(typ)
	return typ, nil
}
