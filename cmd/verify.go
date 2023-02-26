package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/cockroachdb/molt/pkg/verification"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
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
			reporter := &verification.LogReporter{Printf: log.Printf}
			defer reporter.Close()

			ctx := context.Background()
			var conns []verification.Conn
			for _, arg := range args {
				conn, err := pgx.Connect(ctx, arg)
				if err != nil {
					return errors.Wrapf(err, "error connecting to %s", arg)
				}
				conns = append(conns, verification.Conn{ID: verification.ConnID(arg), Conn: conn})
				reporter.Report(verification.StatusReport{Info: fmt.Sprintf("connected to %s", arg)})
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
