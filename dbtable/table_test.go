package dbtable

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDBTableCompare(t *testing.T) {
	for _, tc := range []struct {
		a, b     Name
		expected int
	}{
		{a: Name{Schema: "b", Table: "b"}, b: Name{Schema: "b", Table: "b"}, expected: 0},
		{a: Name{Schema: "b", Table: "b"}, b: Name{Schema: "a", Table: "b"}, expected: 1},
		{a: Name{Schema: "c", Table: "b"}, b: Name{Schema: "e", Table: "b"}, expected: -1},
		{a: Name{Schema: "b", Table: "b"}, b: Name{Schema: "b", Table: "c"}, expected: -1},
		{a: Name{Schema: "b", Table: "d"}, b: Name{Schema: "b", Table: "c"}, expected: 1},
	} {
		t.Run(fmt.Sprintf("%s_%s", tc.a, tc.b), func(t *testing.T) {
			require.Equal(t, tc.expected, DBTable{Name: tc.a}.Compare(DBTable{Name: tc.b}))
			require.Equal(t, -tc.expected, DBTable{Name: tc.b}.Compare(DBTable{Name: tc.a}))
		})
	}

}
