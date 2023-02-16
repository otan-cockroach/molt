package verification

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

type columnName string

type columnMetadata struct {
	columnName columnName
	// TODO: account for geospatial types.
	// TODO: compare typMod, which CRDB does not really support.
	typeOID OID
	notNull bool
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

type verifyTableResult struct {
	Schema                      string
	Table                       string
	RowVerifiable               bool
	MatchingColumns             []columnName
	PrimaryKeyColumns           []columnName
	MismatchingTableDefinitions []MismatchingTableDefinition
}

func verifyCommonTables(
	ctx context.Context, conns []Conn, tables map[ConnID][]TableMetadata,
) ([]verifyTableResult, error) {
	var ret []verifyTableResult
	for tblIdx, truthTbl := range tables[conns[0].ID] {
		res := verifyTableResult{
			Schema: truthTbl.Schema,
			Table:  truthTbl.Table,
		}
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
			for _, targetCol := range compareColumns {
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
				if sourceCol.notNull != targetCol.notNull {
					res.MismatchingTableDefinitions = append(
						res.MismatchingTableDefinitions,
						MismatchingTableDefinition{
							ConnID:        conn.ID,
							TableMetadata: targetTbl,
							Info: fmt.Sprintf(
								"column %s NOT NULL mismatch: %s:%t vs %s:%t",
								targetCol.columnName,
								conns[0].ID,
								sourceCol.notNull,
								conn.ID,
								targetCol.notNull,
							),
						},
					)
					continue
				}
				if sourceCol.typeOID != targetCol.typeOID {
					// TODO(otan): re-use type map.
					aTyp, _ := pgtype.NewMap().TypeForOID(uint32(sourceCol.typeOID))
					bTyp, _ := pgtype.NewMap().TypeForOID(uint32(targetCol.typeOID))
					res.MismatchingTableDefinitions = append(
						res.MismatchingTableDefinitions,
						MismatchingTableDefinition{
							ConnID:        conn.ID,
							TableMetadata: targetTbl,
							Info: fmt.Sprintf(
								"column type mismatch on %s: %s on %s vs %s on %s",
								targetCol.columnName,
								aTyp,
								conns[0].ID,
								bTyp,
								conn.ID,
							),
						},
					)
					delete(comparableColumns, sourceCol.columnName)
				}
				delete(truthMappedCols, targetCol.columnName)
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
				}
			}
			if !currPKSame {
				pkSame = false
				res.MismatchingTableDefinitions = append(
					res.MismatchingTableDefinitions,
					MismatchingTableDefinition{
						ConnID:        conn.ID,
						TableMetadata: targetTbl,
						Info:          "PRIMARY KEY does not match source of truth",
					},
				)
			}
		}

		res.PrimaryKeyColumns = truthPKCols
		// Place PK columns first.
		for _, col := range truthPKCols {
			if _, ok := comparableColumns[col]; !ok {
				res.MatchingColumns = append(res.MatchingColumns, col)
				delete(comparableColumns, col)
			}
		}
		for _, col := range truthCols {
			if _, ok := comparableColumns[col.columnName]; ok {
				res.MatchingColumns = append(res.MatchingColumns, col.columnName)
			}
		}
		res.RowVerifiable = pkSame && len(truthPKCols) > 0
		ret = append(ret, res)
	}
	return ret, nil
}
