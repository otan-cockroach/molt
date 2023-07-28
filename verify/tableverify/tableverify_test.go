package tableverify

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/verify/inconsistency"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

func TestVerifyTable(t *testing.T) {
	conns := dbconn.OrderedConns{
		dbconn.MakeFakeConn("aaa"),
		dbconn.MakeFakeConn("bbb"),
	}
	for _, tc := range []struct {
		desc      string
		cmpTables [2]dbtable.DBTable
		pkCols    [2][]tree.Name
		columns   [2][]Column
		expected  Result
	}{
		{
			desc: "success",
			cmpTables: [2]dbtable.DBTable{
				{Name: dbtable.Name{Schema: "public", Table: "tbl_name"}},
				{Name: dbtable.Name{Schema: "public", Table: "tbl_name"}},
			},
			pkCols: [2][]tree.Name{
				{"id"},
				{"id"},
			},
			columns: [2][]Column{
				{
					{Name: "id", OID: oid.T_int4, NotNull: true},
					{Name: "txt", OID: oid.T_text, NotNull: true},
				},
				{
					{Name: "id", OID: oid.T_int4, NotNull: true},
					{Name: "txt", OID: oid.T_text, NotNull: true},
				},
			},
			expected: Result{
				RowVerifiable: true,
				VerifiedTable: dbtable.VerifiedTable{
					Name:              dbtable.Name{Schema: "public", Table: "tbl_name"},
					PrimaryKeyColumns: []tree.Name{"id"},
					Columns:           []tree.Name{"id", "txt"},
					ColumnOIDs:        [2][]oid.Oid{{oid.T_int4, oid.T_text}, {oid.T_int4, oid.T_text}},
				},
			},
		},
		{
			desc: "missing primary key on source",
			cmpTables: [2]dbtable.DBTable{
				{Name: dbtable.Name{Schema: "public", Table: "tbl_name"}},
				{Name: dbtable.Name{Schema: "public", Table: "tbl_name"}},
			},
			pkCols: [2][]tree.Name{
				{},
				{"id"},
			},
			columns: [2][]Column{
				{
					{Name: "id", OID: oid.T_int4, NotNull: true},
					{Name: "txt", OID: oid.T_text, NotNull: true},
				},
				{
					{Name: "id", OID: oid.T_int4, NotNull: true},
					{Name: "txt", OID: oid.T_text, NotNull: true},
				},
			},
			expected: Result{
				VerifiedTable: dbtable.VerifiedTable{
					Name:              dbtable.Name{Schema: "public", Table: "tbl_name"},
					PrimaryKeyColumns: []tree.Name{},
					Columns:           []tree.Name{"id", "txt"},
					ColumnOIDs:        [2][]oid.Oid{{oid.T_int4, oid.T_text}, {oid.T_int4, oid.T_text}},
				},
				// TODO: add more tests after we make mismatches more specialised.
				MismatchingTableDefinitions: []inconsistency.MismatchingTableDefinition{
					{
						DBTable: dbtable.DBTable{Name: dbtable.Name{Schema: "public", Table: "tbl_name"}, OID: 0x0},
						Info:    "missing a PRIMARY KEY - results cannot be compared"},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			res, err := verifyTable(ctx, conns, tc.cmpTables, tc.pkCols, tc.columns)
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
		})
	}
}
