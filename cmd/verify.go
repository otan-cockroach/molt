package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/dbconn"
	"github.com/cockroachdb/molt/pkg/verification"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var (
	// TODO: sanity check bounds.
	flagVerifyConcurrency  int
	flagVerifyTableSplits  int
	flagVerifyRowBatchSize int

	verifyCmd = &cobra.Command{
		Use:   "verify",
		Short: "Verify table schemas and row data align.",
		Long:  `verify ensure table schemas and row data between the two databases are aligned.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cw := zerolog.NewConsoleWriter()
			reporter := &verification.LogReporter{Logger: zerolog.New(cw)}
			defer reporter.Close()

			ctx := context.Background()
			var conns []dbconn.Conn
			for _, arg := range args {
				var preferredID dbconn.ID
				connStr := arg
				splitArgs := strings.SplitN(arg, "===", 2)
				if len(splitArgs) == 2 {
					preferredID, connStr = dbconn.ID(splitArgs[0]), splitArgs[1]
				}
				reporter.Report(verification.StatusReport{Info: fmt.Sprintf("connecting to %s", connStr)})
				conn, err := dbconn.Connect(ctx, preferredID, connStr)
				if err != nil {
					return err
				}
				conns = append(conns, conn)
				reporter.Report(verification.StatusReport{Info: fmt.Sprintf("connected to %s as %s", connStr, conn.ID())})
			}

			reporter.Report(verification.StatusReport{Info: "verification in progress"})
			if err := verification.Verify(
				ctx,
				conns,
				reporter,
				verification.WithConcurrency(flagVerifyConcurrency),
				verification.WithTableSplits(flagVerifyTableSplits),
				verification.WithRowBatchSize(flagVerifyRowBatchSize),
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
		16,
		"number of shards to process at a time",
	)
	verifyCmd.PersistentFlags().IntVar(
		&flagVerifyTableSplits,
		"table_splits",
		16,
		"number of ways to break down a table",
	)
	verifyCmd.PersistentFlags().IntVar(
		&flagVerifyRowBatchSize,
		"row_batch_size",
		20000,
		"number of rows to get from a table at a time",
	)
	rootCmd.AddCommand(verifyCmd)
}
