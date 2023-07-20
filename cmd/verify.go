package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/verification"
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

	verifyCmd = &cobra.Command{
		Use:   "verify",
		Short: "Verify table schemas and row data align.",
		Long:  `Verify ensure table schemas and row data between the two databases are aligned.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cw := zerolog.NewConsoleWriter()
			logger := zerolog.New(cw)

			reporter := verification.CombinedReporter{}
			reporter.Reporters = append(reporter.Reporters, &verification.LogReporter{Logger: logger})
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
				reporter.Report(verification.StatusReport{Info: fmt.Sprintf("connected to %s", conn.ID())})
			}
			if flagVerifyFixup {
				reporter.Reporters = append(reporter.Reporters, &verification.FixReporter{
					Conn:   conns[1],
					Logger: logger,
				})
			}

			verification.TimingEnabled = true
			reporter.Report(verification.StatusReport{Info: "verification in progress"})
			if err := verification.Verify(
				ctx,
				conns,
				reporter,
				verification.WithConcurrency(flagVerifyConcurrency),
				verification.WithTableSplits(flagVerifyTableSplits),
				verification.WithRowBatchSize(flagVerifyRowBatchSize),
				verification.WithContinuous(flagVerifyContinuous, flagVerifyContinuousPause),
			); err != nil {
				return errors.Wrapf(err, "error verifying")
			}
			reporter.Report(verification.StatusReport{Info: "verification complete"})
			return nil
		},
	}
)

func init() {
	verifyCmd.PersistentFlags().IntVar(
		&flagVerifyConcurrency,
		"concurrency",
		1,
		"number of shards to process at a time",
	)
	verifyCmd.PersistentFlags().IntVar(
		&flagVerifyTableSplits,
		"table_splits",
		1,
		"number of shards to break down each table into whilst doing row-based verification",
	)
	verifyCmd.PersistentFlags().IntVar(
		&flagVerifyRowBatchSize,
		"row_batch_size",
		20000,
		"number of rows to get from a table at a time",
	)
	verifyCmd.PersistentFlags().BoolVar(
		&flagVerifyFixup,
		"fixup",
		false,
		"whether to fix up any rows",
	)
	if err := verifyCmd.PersistentFlags().MarkHidden("fixup"); err != nil {
		panic(err)
	}
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
	rootCmd.AddCommand(verifyCmd)
}
