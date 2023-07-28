package verify

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/retry"
	"github.com/cockroachdb/molt/verify"
	"github.com/cockroachdb/molt/verify/inconsistency"
	"github.com/cockroachdb/molt/verify/rowverify"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	// TODO: sanity check bounds.
	var (
		verifyConcurrency              int
		verifyTableSplits              int
		verifyRowBatchSize             int
		verifyFixup                    bool
		verifyContinuousPause          time.Duration
		verifyContinuous               bool
		verifyLive                     bool
		verifyLiveVerificationSettings = rowverify.LiveReverificationSettings{
			MaxBatchSize:  100,
			FlushInterval: time.Second,
			RetrySettings: retry.Settings{
				InitialBackoff: 250 * time.Millisecond,
				Multiplier:     2,
				MaxBackoff:     time.Second,
				MaxRetries:     5,
			},
		}
	)

	verifyCmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify table schemas and row data align.",
		Long:  `Verify ensure table schemas and row data between the two databases are aligned.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cw := zerolog.NewConsoleWriter()
			logger := zerolog.New(cw)

			reporter := inconsistency.CombinedReporter{}
			reporter.Reporters = append(reporter.Reporters, &inconsistency.LogReporter{Logger: logger})
			defer reporter.Close()

			ctx := context.Background()
			var conns dbconn.OrderedConns
			if len(args) != 2 {
				return errors.Newf("expected two connections")
			}
			for i, arg := range args {
				var preferredID dbconn.ID
				connStr := arg
				splitArgs := strings.SplitN(arg, "===", 2)
				if len(splitArgs) == 2 {
					preferredID, connStr = dbconn.ID(splitArgs[0]), splitArgs[1]
				}
				conn, err := dbconn.Connect(ctx, preferredID, connStr)
				if err != nil {
					return err
				}
				conns[i] = conn
				reporter.Report(inconsistency.StatusReport{Info: fmt.Sprintf("connected to %s", conn.ID())})
			}
			if verifyFixup {
				fixupConn, err := conns[1].Clone(ctx)
				if err != nil {
					panic(err)
				}
				reporter.Reporters = append(reporter.Reporters, &inconsistency.FixReporter{
					Conn:   fixupConn,
					Logger: logger,
				})
			}

			reporter.Report(inconsistency.StatusReport{Info: "verification in progress"})
			if err := verify.Verify(
				ctx,
				conns,
				logger,
				reporter,
				verify.WithConcurrency(verifyConcurrency),
				verify.WithTableSplits(verifyTableSplits),
				verify.WithRowBatchSize(verifyRowBatchSize),
				verify.WithContinuous(verifyContinuous, verifyContinuousPause),
				verify.WithLive(verifyLive, verifyLiveVerificationSettings),
			); err != nil {
				return errors.Wrapf(err, "error verifying")
			}
			reporter.Report(inconsistency.StatusReport{Info: "verification complete"})
			return nil
		},
	}

	verifyCmd.PersistentFlags().IntVar(
		&verifyConcurrency,
		"concurrency",
		0,
		"number of tables to process at a time (defaults to number of CPUs)",
	)
	verifyCmd.PersistentFlags().IntVar(
		&verifyTableSplits,
		"table-splits",
		1,
		"number of shards to break down each table into whilst doing row-based verification",
	)
	verifyCmd.PersistentFlags().IntVar(
		&verifyRowBatchSize,
		"row-batch-size",
		20000,
		"number of rows to get from a table at a time",
	)
	verifyCmd.PersistentFlags().BoolVar(
		&verifyFixup,
		"fixup",
		false,
		"whether to fix up any rows",
	)
	verifyCmd.PersistentFlags().DurationVar(
		&verifyContinuousPause,
		"continuous-pause-duration",
		0,
		"pause between continuous runs",
	)
	verifyCmd.PersistentFlags().BoolVar(
		&verifyContinuous,
		"continuous",
		false,
		"whether verification should continuously run on each shard",
	)
	verifyCmd.PersistentFlags().BoolVar(
		&verifyLive,
		"live",
		false,
		"enables live mode, which attempts to account for rows that can change in value by retrying them before marking them as an inconsistency",
	)
	verifyCmd.PersistentFlags().IntVar(
		&verifyLiveVerificationSettings.MaxBatchSize,
		"live-max-batch-size",
		verifyLiveVerificationSettings.MaxBatchSize,
		"maximum number of rows to retry at a time in live verification mode",
	)
	verifyCmd.PersistentFlags().DurationVar(
		&verifyLiveVerificationSettings.FlushInterval,
		"live-flush-interval",
		verifyLiveVerificationSettings.FlushInterval,
		"maximum amount of time to wait before rows can start being retried",
	)
	verifyCmd.PersistentFlags().IntVar(
		&verifyLiveVerificationSettings.RetrySettings.MaxRetries,
		"live-retries-max-iterations",
		verifyLiveVerificationSettings.RetrySettings.MaxRetries,
		"maximum number of retries live reverification should run before marking rows as inconsistent",
	)
	verifyCmd.PersistentFlags().DurationVar(
		&verifyLiveVerificationSettings.RetrySettings.MaxBackoff,
		"live-retry-max-backoff",
		verifyLiveVerificationSettings.RetrySettings.MaxBackoff,
		"maximum amount of time live reverification should take before retrying",
	)
	verifyCmd.PersistentFlags().DurationVar(
		&verifyLiveVerificationSettings.RetrySettings.InitialBackoff,
		"live-retry-initial-backoff",
		verifyLiveVerificationSettings.RetrySettings.InitialBackoff,
		"amount of time live verification should initially backoff for before retrying",
	)
	verifyCmd.PersistentFlags().IntVar(
		&verifyLiveVerificationSettings.RetrySettings.Multiplier,
		"live-retry-multiplier",
		verifyLiveVerificationSettings.RetrySettings.Multiplier,
		"multiplier applied to retry after each unsuccessful live reverification run",
	)
	for _, hidden := range []string{"fixup", "table-splits"} {
		if err := verifyCmd.PersistentFlags().MarkHidden(hidden); err != nil {
			panic(err)
		}
	}
	return verifyCmd
}
