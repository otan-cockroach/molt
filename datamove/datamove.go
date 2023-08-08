package datamove

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/verify/dbverify"
	"github.com/cockroachdb/molt/verify/tableverify"
	"github.com/rs/zerolog"
)

type Config struct {
	FlushSize int
	Cleanup   bool
	Live      bool
	Truncate  bool
}

func DataMove(
	ctx context.Context,
	cfg Config,
	logger zerolog.Logger,
	conns dbconn.OrderedConns,
	src datamovestore.Store,
	tableFilter dbverify.FilterConfig,
) error {
	if cfg.FlushSize == 0 {
		cfg.FlushSize = src.DefaultFlushBatchSize()
	}
	defer func() {
		if cfg.Cleanup {
			if err := src.Cleanup(ctx); err != nil {
				logger.Err(err).Msgf("error marking object for cleanup")
			}
		}
	}()
	logger.Debug().
		Int("flush_size", cfg.FlushSize).
		Str("store", fmt.Sprintf("%T", src)).
		Msg("initial config")

	logger.Info().Msgf("checking database details")
	dbTables, err := dbverify.Verify(ctx, conns)
	if err != nil {
		return err
	}
	if dbTables, err = dbverify.FilterResult(tableFilter, dbTables); err != nil {
		return err
	}
	for _, tbl := range dbTables.ExtraneousTables {
		logger.Warn().
			Str("table", tbl.SafeString()).
			Msgf("ignoring table as it is missing a definition on the source")
	}
	for _, tbl := range dbTables.MissingTables {
		logger.Warn().
			Str("table", tbl.SafeString()).
			Msgf("ignoring table as it is missing a definition on the target")
	}
	for _, tbl := range dbTables.Verified {
		logger.Info().
			Str("source_table", tbl[0].SafeString()).
			Str("target_table", tbl[1].SafeString()).
			Msgf("found matching table")
	}

	logger.Info().Msgf("verifying common tables")
	tables, err := tableverify.VerifyCommonTables(ctx, conns, dbTables.Verified)
	if err != nil {
		return err
	}
	logger.Info().Msgf("establishing snapshot")
	sqlSrc, err := InferExportSource(ctx, conns[0])
	if err != nil {
		return err
	}
	logger.Info().
		Int("num_tables", len(tables)).
		Str("cdc_cursor", sqlSrc.CDCCursor()).
		Msgf("starting data movement")

	importErrCh := make(chan error)
	var numImported int
	for idx, table := range tables {
		// Stick in a defer so defers close ASAP.
		if err := func() error {
			tableStartTime := time.Now()
			table := table
			logger := logger.With().Str("table", table.SafeString()).Logger()

			for _, col := range table.MismatchingTableDefinitions {
				logger.Warn().
					Str("reason", col.Info).
					Msgf("not migrating column %s as it mismatches", col.Name)
			}
			if !table.RowVerifiable {
				logger.Error().Msgf("table %s do not have matching primary keys, cannot migrate", table.SafeString())
				return nil
			}

			logger.Info().
				Msgf("data extraction phase starting")

			e, err := Export(ctx, logger, sqlSrc, src, table.VerifiedTable, cfg.FlushSize)
			if err != nil {
				return err
			}

			cleanupFunc := func() {
				if cfg.Cleanup {
					for _, r := range e.Resources {
						if err := r.MarkForCleanup(ctx); err != nil {
							logger.Err(err).Msgf("error cleaning up resource")
						}
					}
				}
			}

			logger.Info().
				Int("num_rows", e.NumRows).
				Dur("export_duration", e.EndTime.Sub(e.StartTime)).
				Msgf("data extraction from source complete")

			if src.CanBeTarget() {
				if idx > 0 {
					if err := <-importErrCh; err != nil {
						return errors.Wrapf(err, "error importing %s", tables[idx-1].SafeString())
					}
				}
				// Start async goroutine to import in data concurrently.
				go func() {
					// It is important that we only cleanup when the import of data is complete.
					importErrCh <- func() error {
						if cfg.Truncate {
							logger.Info().
								Msgf("truncating table")
							_, err := conns[1].(*dbconn.PGConn).Conn.Exec(ctx, "TRUNCATE TABLE "+table.SafeString())
							if err != nil {
								return err
							}
						}
						defer cleanupFunc()

						logger.Info().
							Msgf("starting data import on target")

						var importDuration time.Duration
						if !cfg.Live {
							r, err := Import(ctx, conns[1], logger, table.VerifiedTable, e.Resources)
							if err != nil {
								return err
							}
							importDuration = r.EndTime.Sub(r.StartTime)
						} else {
							r, err := Copy(ctx, conns[1], logger, table.VerifiedTable, e.Resources)
							if err != nil {
								return err
							}
							importDuration = r.EndTime.Sub(r.StartTime)
						}
						numImported++
						logger.Info().
							Dur("net_duration", time.Since(tableStartTime)).
							Dur("import_duration", importDuration).
							Str("cdc_cursor", e.CDCCursor).
							Msgf("data import on target for table complete")
						return nil
					}()
				}()
			} else {
				cleanupFunc()
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	if len(tables) > 0 {
		if err := <-importErrCh; err != nil {
			return errors.Wrapf(err, "error importing %s", tables[len(tables)-1].SafeString())
		}
	}
	logger.Info().
		Str("cdc_cursor", sqlSrc.CDCCursor()).
		Int("num_imported", numImported).
		Msgf("data movement complete")
	return nil
}
