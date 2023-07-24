package verification

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

func (tm TableMetadata) Compare(o TableMetadata) int {
	if c := strings.Compare(string(tm.Schema), string(o.Schema)); c != 0 {
		return c
	}
	return strings.Compare(string(tm.Table), string(o.Table))
}

func (tm TableMetadata) Less(o TableMetadata) bool {
	return tm.Compare(o) < 0
}

const DefaultConcurrency = 8
const DefaultRowBatchSize = 1000
const DefaultTableSplits = 8

type VerifyOpt func(*verifyOpts)

type verifyOpts struct {
	concurrency     int
	rowBatchSize    int
	tableSplits     int
	continuous      bool
	continuousPause time.Duration
	live            bool
}

func WithConcurrency(c int) VerifyOpt {
	return func(o *verifyOpts) {
		o.concurrency = c
	}
}

func WithRowBatchSize(c int) VerifyOpt {
	return func(o *verifyOpts) {
		o.rowBatchSize = c
	}
}

func WithTableSplits(c int) VerifyOpt {
	return func(o *verifyOpts) {
		o.tableSplits = c
	}
}

func WithContinuous(c bool, pauseLength time.Duration) VerifyOpt {
	return func(o *verifyOpts) {
		o.continuous = c
		o.continuousPause = pauseLength
	}
}

func WithLive(l bool) VerifyOpt {
	return func(o *verifyOpts) {
		o.live = l
	}
}

// Verify verifies the given connections have matching tables and contents.
func Verify(
	ctx context.Context,
	conns dbconn.OrderedConns,
	logger zerolog.Logger,
	reporter Reporter,
	inOpts ...VerifyOpt,
) error {
	opts := verifyOpts{
		concurrency:  DefaultConcurrency,
		rowBatchSize: DefaultRowBatchSize,
		tableSplits:  DefaultTableSplits,
	}
	for _, applyOpt := range inOpts {
		applyOpt(&opts)
	}

	ret, err := verifyDatabaseTables(ctx, conns)
	if err != nil {
		return errors.Wrap(err, "error comparing database tables")
	}

	for _, missingTable := range ret.missingTables {
		reporter.Report(missingTable)
	}
	for _, extraneousTable := range ret.extraneousTables {
		reporter.Report(extraneousTable)
	}

	// Grab columns for each table on both sides.
	tbls, err := verifyCommonTables(ctx, conns, ret.verified)
	if err != nil {
		return err
	}

	// Report mismatching table definitions.
	for _, tbl := range tbls {
		for _, d := range tbl.MismatchingTableDefinitions {
			reporter.Report(d)
		}
	}

	shards := make([]TableShard, 0, len(tbls))
	for _, tbl := range tbls {
		if !tbl.RowVerifiable {
			logger.Warn().Msgf("skipping unverifiable table %s.%s", tbl.Schema, tbl.Table)
			continue
		}
		// Get and first and last of each PK.
		tableShards, err := shardTable(ctx, conns[0], tbl, reporter, opts.tableSplits)
		if err != nil {
			return errors.Wrapf(err, "error splitting tables")
		}
		shards = append(shards, tableShards...)
	}

	numGoroutines := opts.concurrency
	if opts.continuous {
		if opts.concurrency != 0 && opts.concurrency < len(shards) {
			logger.Warn().Msgf("--concurrency is set to a higher value %d so that continuous mode can run successfully", len(shards))
		}
		// For continuous mode, we default to running each shard on a continous loop.
		numGoroutines = len(shards)
	} else if numGoroutines == 0 {
		numGoroutines = runtime.NumCPU()
		logger.Debug().Int("concurrency", numGoroutines).
			Msgf("no concurrency set; defaulting to number of CPUs")
	}

	if numGoroutines > runtime.NumCPU()*4 {
		logger.Warn().Msgf(
			"running %d row verifications concurrently, which may thrash your CPU (consider reducing using --concurrency or using less tables)",
			numGoroutines,
		)
	}

	// Compare rows up to the numGoroutines specified.
	g, _ := errgroup.WithContext(ctx)
	workQueue := make(chan TableShard)
	for goroutineIdx := 0; goroutineIdx < numGoroutines; goroutineIdx++ {
		g.Go(func() error {
			for {
				shard, ok := <-workQueue
				if !ok {
					return nil
				}
				for runNum := 1; opts.continuous || runNum <= 1; runNum++ {
					msg := fmt.Sprintf(
						"starting verify on %s.%s, shard %d/%d",
						shard.Schema,
						shard.Table,
						shard.ShardNum,
						shard.TotalShards,
					)
					if opts.continuous {
						msg += fmt.Sprintf(", run #%d", runNum)
					}
					if shard.TotalShards > 1 {
						msg += ", range: ["
						if len(shard.StartPKVals) > 0 {
							for i, val := range shard.StartPKVals {
								if i > 0 {
									msg += ","
								}
								msg += val.String()
							}
						} else {
							msg += "<beginning>"
						}
						msg += " - "
						if len(shard.EndPKVals) > 0 {
							for i, val := range shard.EndPKVals {
								if i > 0 {
									msg += ", "
								}
								msg += val.String()
							}
							msg += ")"
						} else {
							msg += "<end>]"
						}
					}
					reporter.Report(StatusReport{
						Info: msg,
					})
					if err := verifyRowShard(ctx, conns, reporter, logger, opts.rowBatchSize, shard, opts.live); err != nil {
						logger.Err(err).
							Str("schema", string(shard.Schema)).
							Str("table", string(shard.Table)).
							Msgf("error verifying rows")
					}
					time.Sleep(opts.continuousPause)
				}
			}
		})
	}
	for _, shard := range shards {
		workQueue <- shard
	}
	close(workQueue)
	return g.Wait()
}

func verifyRowShard(
	ctx context.Context,
	conns dbconn.OrderedConns,
	reporter Reporter,
	logger zerolog.Logger,
	rowBatchSize int,
	tbl TableShard,
	live bool,
) error {
	// Copy connections over naming wise, but initialize a new connection
	// for each table.
	var workerConns dbconn.OrderedConns
	for i := range workerConns {
		// Make a copy of i so the worker closes correctly.
		i := i
		var err error
		workerConns[i], err = conns[i].Clone(ctx)
		if err != nil {
			return errors.Wrap(err, "error establishing connection to compare")
		}
		defer func() {
			_ = workerConns[i].Close(ctx)
		}()
	}
	return verifyRowsOnShard(ctx, workerConns, tbl, rowBatchSize, reporter, logger, live)
}
