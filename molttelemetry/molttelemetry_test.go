package molttelemetry_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/molt/molttelemetry"
	"github.com/cockroachdb/molt/testutils"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestReportTelemetry(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		telemetry []string
	}{
		{desc: "no items", telemetry: []string{}},
		{desc: "one item", telemetry: []string{"a"}},
		{desc: "three items", telemetry: []string{"a", "b", "c"}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			connStr := testutils.CRDBConnStr()
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)

			before := countTelemetry(t, conn)
			defer molttelemetry.RegisterConnString(connStr)()
			require.NoError(t, molttelemetry.ReportTelemetry(tc.telemetry...))
			after := countTelemetry(t, conn)
			for _, it := range tc.telemetry {
				require.Equal(t, before[molttelemetry.TelemetryKey(it)]+1, after[molttelemetry.TelemetryKey(it)])
			}
		})
	}
}

func countTelemetry(t *testing.T, conn *pgx.Conn) map[string]int {
	ret := make(map[string]int)
	ctx := context.Background()
	rows, err := conn.Query(ctx, "select feature_name, usage_count from crdb_internal.feature_usage")
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var fn string
		var val int
		require.NoError(t, rows.Scan(&fn, &val))
		ret[fn] = val
	}
	require.NoError(t, rows.Err())
	return ret
}
