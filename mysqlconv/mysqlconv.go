package mysqlconv

import (
	"database/sql"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pkg/errors"
)

func MySQLASTColumnField(name tree.Name) *ast.ColumnNameExpr {
	return &ast.ColumnNameExpr{
		Name: &ast.ColumnName{
			Name: model.NewCIStr(string(name)),
		},
	}
}

// ScanRowDynamicTypes is intended to read a rows result where the types
// cannot be declared upfront.
func ScanRowDynamicTypes(
	rows *sql.Rows, typMap *pgtype.Map, typOIDs []oid.Oid,
) (tree.Datums, error) {
	typs, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	// Since we are passing in "valPtrs", golang doesn't like if we use
	// anything other data type other than []byte. This is extremely annoying and means
	// we have to parse strings.
	vals := make([][]byte, len(typs))
	valPtrs := make([]any, len(typs))
	for i := range typs {
		valPtrs[i] = &vals[i]
	}
	if err := rows.Scan(valPtrs...); err != nil {
		return nil, errors.Wrap(err, "failed to scan row")
	}
	return ConvertRowValues(typMap, vals, typOIDs)
}
