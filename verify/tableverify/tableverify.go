package tableverify

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/verify/inconsistency"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
)

type Result struct {
	RowVerifiable bool
	dbtable.VerifiedTable
	MismatchingTableDefinitions []inconsistency.MismatchingTableDefinition
}

func VerifyCommonTables(
	ctx context.Context, conns dbconn.OrderedConns, allTables [][2]dbtable.DBTable,
) ([]Result, error) {
	var ret []Result

	for _, cmpTables := range allTables {
		pkCols, err := getPrimaryKeysForTables(ctx, conns, cmpTables)
		if err != nil {
			return nil, err
		}
		columns, err := getColumnsForTables(ctx, conns, cmpTables)
		if err != nil {
			return nil, err
		}
		res, err := verifyTable(ctx, conns, cmpTables, pkCols, columns)
		if err != nil {
			return nil, err
		}
		ret = append(ret, res)
	}
	return ret, nil
}

func verifyTable(
	ctx context.Context,
	conns dbconn.OrderedConns,
	cmpTables [2]dbtable.DBTable,
	pkCols [2][]tree.Name,
	columns [2][]columnMetadata,
) (Result, error) {
	truthTbl := cmpTables[0]
	var columnOIDMap [2]map[tree.Name]oid.Oid
	for i := range columnOIDMap {
		columnOIDMap[i] = make(map[tree.Name]oid.Oid)
	}
	res := Result{
		VerifiedTable: dbtable.VerifiedTable{
			Name: dbtable.Name{
				Schema: truthTbl.Schema,
				Table:  truthTbl.Table,
			},
		},
	}
	truthCols := columns[0]
	columnOIDMap[0] = make(map[tree.Name]oid.Oid)
	for _, truthCol := range truthCols {
		columnOIDMap[0][truthCol.columnName] = truthCol.typeOID
	}

	pkSame := true
	truthPKCols := pkCols[0]
	if len(truthPKCols) == 0 {
		res.MismatchingTableDefinitions = append(
			res.MismatchingTableDefinitions,
			inconsistency.MismatchingTableDefinition{
				DBTable: truthTbl,
				Info:    "missing a PRIMARY KEY - results cannot be compared",
			},
		)
	}

	comparableColumns := mapColumns(truthCols)
	truthMappedCols := mapColumns(truthCols)
	targetTbl := cmpTables[1]
	compareColumns := columns[1]
	columnOIDMap[1] = make(map[tree.Name]oid.Oid)
	for _, targetCol := range compareColumns {
		columnOIDMap[1][targetCol.columnName] = targetCol.typeOID
		sourceCol, ok := truthMappedCols[targetCol.columnName]
		if !ok {
			res.MismatchingTableDefinitions = append(
				res.MismatchingTableDefinitions,
				inconsistency.MismatchingTableDefinition{
					DBTable: targetTbl,
					Info:    fmt.Sprintf("extraneous column %s found", targetCol.columnName),
				},
			)
			continue
		}

		delete(truthMappedCols, targetCol.columnName)

		if sourceCol.notNull != targetCol.notNull {
			res.MismatchingTableDefinitions = append(
				res.MismatchingTableDefinitions,
				inconsistency.MismatchingTableDefinition{
					DBTable: targetTbl,
					Info: fmt.Sprintf(
						"column %s NOT NULL mismatch: %t vs %t",
						targetCol.columnName,
						sourceCol.notNull,
						targetCol.notNull,
					),
				},
			)
		}
		truthTyp, err := dbconn.GetDataType(ctx, conns[0], sourceCol.typeOID)
		if err != nil {
			return Result{}, err
		}
		compareTyp, err := dbconn.GetDataType(ctx, conns[1], targetCol.typeOID)
		if err != nil {
			return Result{}, err
		}
		if !comparableType(truthTyp, compareTyp) {
			res.MismatchingTableDefinitions = append(
				res.MismatchingTableDefinitions,
				inconsistency.MismatchingTableDefinition{
					DBTable: targetTbl,
					Info: fmt.Sprintf(
						"column type mismatch on %s: %s vs %s",
						targetCol.columnName,
						truthTyp.Name,
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
			inconsistency.MismatchingTableDefinition{
				DBTable: targetTbl,
				Info: fmt.Sprintf(
					"missing column %s",
					colName,
				),
			},
		)
		delete(comparableColumns, colName)
	}

	targetPKCols := pkCols[1]

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
			inconsistency.MismatchingTableDefinition{
				DBTable: targetTbl,
				Info:    "PRIMARY KEY does not match source of truth (columns and types must match)",
			},
		)
	}

	res.PrimaryKeyColumns = truthPKCols
	// Place PK columns first.
	for _, col := range truthPKCols {
		if _, ok := comparableColumns[col]; ok {
			res.Columns = append(res.Columns, col)
			for i := 0; i < 2; i++ {
				res.ColumnOIDs[i] = append(res.ColumnOIDs[i], columnOIDMap[i][col])
			}
			delete(comparableColumns, col)
		}
	}
	// Then every other column.
	for _, col := range truthCols {
		if _, ok := comparableColumns[col.columnName]; ok {
			res.Columns = append(res.Columns, col.columnName)
			for i := 0; i < 2; i++ {
				res.ColumnOIDs[i] = append(res.ColumnOIDs[i], columnOIDMap[i][col.columnName])
			}
		}
	}
	res.RowVerifiable = pkSame && len(truthPKCols) > 0
	return res, nil
}

func comparableType(a, b *pgtype.Type) bool {
	if a.Name == b.Name {
		return true
	}
	aTyp, ok := types.OidToType[oid.Oid(a.OID)]
	if !ok {
		return false
	}
	bTyp, ok := types.OidToType[oid.Oid(b.OID)]
	if !ok {
		return false
	}
	return aTyp.Equivalent(bTyp)
}
