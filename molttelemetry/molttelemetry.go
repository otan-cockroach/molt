package molttelemetry

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
)

var connStr string

// RegisterConnString registers a connection string to Cockroach for telemetry,
// returning a function which undoes the global state.
func RegisterConnString(str string) func() {
	old := connStr
	connStr = str
	return func() { connStr = old }
}

// ReportTelemetryAsync reports telemetry asynchronously.
func ReportTelemetryAsync(logger zerolog.Logger, telemetry ...string) {
	go func() {
		if err := ReportTelemetry(telemetry...); err != nil {
			logger.Warn().Err(err).Strs("telemetry", telemetry).Msgf("error reporting telemetry")
			return
		}
		logger.Trace().Strs("telemetry", telemetry).Msgf("reported telemetry")
	}()
}

// ReportTelemetry reports telemetry using crdb_internal.increment_feature_counter.
// RegisterConnString must be called beforehand.
func ReportTelemetry(telemetry ...string) error {
	if len(telemetry) == 0 {
		return nil
	}
	if connStr == "" {
		return errors.AssertionFailedf("telemetry called without a registered conn string")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return err
	}

	if err := func() error {
		args := make([]interface{}, len(telemetry))
		q := "SELECT "
		for i := range telemetry {
			if i > 0 {
				q += ", "
			}
			q += fmt.Sprintf("crdb_internal.increment_feature_counter($%d)", i+1)
			args[i] = telemetry[i]
		}
		_, err := conn.Exec(ctx, q, args...)
		return err
	}(); err != nil {
		return errors.CombineErrors(err, conn.Close(ctx))
	}
	return conn.Close(ctx)
}

func TelemetryKey(feature string) string {
	sum := sha256.Sum256([]byte(feature))
	return fmt.Sprintf("sql.hashed.%x", sum)
}
