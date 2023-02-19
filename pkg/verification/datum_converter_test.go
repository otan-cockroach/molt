package verification

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestConvertNumeric(t *testing.T) {
	for _, tc := range []string{
		"NaN",
		"Infinity",
		"-Infinity",
		"-1.55",
		"1.578384378473",
	} {
		t.Run(tc, func(t *testing.T) {
			var inVal pgtype.Numeric
			require.NoError(t, inVal.Scan(tc))
			require.True(t, inVal.Valid)

			expected, err := tree.ParseDDecimal(tc)
			require.NoError(t, err)

			n, err := convertNumeric(inVal)
			require.NoError(t, err)
			require.Equal(t, expected, n)
		})
	}
}
