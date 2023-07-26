package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/verify"
	"github.com/cockroachdb/molt/verify/inconsistency"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var (
	// TODO: sanity check bounds.
	flagVerifyConcurrency     int
	flagVerifyTableSplits     int
	flagVerifyRowBatchSize    int
	flagVerifyFixup           bool
	flagVerifyContinuousPause time.Duration
	flagVerifyContinuous      bool
	flagVerifyLive            bool

	verifyCmd = &cobra.Command{
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
			if flagVerifyFixup {
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
				verify.WithConcurrency(flagVerifyConcurrency),
				verify.WithTableSplits(flagVerifyTableSplits),
				verify.WithRowBatchSize(flagVerifyRowBatchSize),
				verify.WithContinuous(flagVerifyContinuous, flagVerifyContinuousPause),
				verify.WithLive(flagVerifyLive),
			); err != nil {
				return errors.Wrapf(err, "error verifying")
			}
			reporter.Report(inconsistency.StatusReport{Info: "verification complete"})
			return nil
		},
	}
)

func init() {
	verifyCmd.PersistentFlags().IntVar(
		&flagVerifyConcurrency,
		"concurrency",
		0,
		"number of tables to process at a time (defaults to number of CPUs)",
	)
	verifyCmd.PersistentFlags().IntVar(
		&flagVerifyTableSplits,
		"table-splits",
		1,
		"number of shards to break down each table into whilst doing row-based verification",
	)
	verifyCmd.PersistentFlags().IntVar(
		&flagVerifyRowBatchSize,
		"row-batch-size",
		20000,
		"number of rows to get from a table at a time",
	)
	verifyCmd.PersistentFlags().BoolVar(
		&flagVerifyFixup,
		"fixup",
		false,
		"whether to fix up any rows",
	)
	verifyCmd.PersistentFlags().DurationVar(
		&flagVerifyContinuousPause,
		"continuous-pause-duration",
		0,
		"pause between continuous runs",
	)
	verifyCmd.PersistentFlags().BoolVar(
		&flagVerifyContinuous,
		"continuous",
		false,
		"whether verification should continuously run on each shard",
	)
	verifyCmd.PersistentFlags().BoolVar(
		&flagVerifyLive,
		"live",
		false,
		"whether verification should attempt to account for rows that can change in value",
	)
	rootCmd.AddCommand(verifyCmd)
	for _, hidden := range []string{"fixup", "table-splits", "live"} {
		if err := verifyCmd.PersistentFlags().MarkHidden(hidden); err != nil {
			panic(err)
		}
	}
}
