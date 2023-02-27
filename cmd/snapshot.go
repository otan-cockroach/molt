package cmd

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/dbconn"
	"github.com/cockroachdb/molt/pkg/verification"
	"github.com/spf13/cobra"
)

var (
	// TODO: sanity check bounds.
	flagSnapshotConcurrency    int
	flagSnapshotTableSplits    int
	flagSnapshotReadBatchSize  int
	flagSnapshotWriteBatchSize int

	snapshotCmd = &cobra.Command{
		Use:   "snapshot",
		Short: "Snapshots a table and it's rows.",
		Long:  "Snapshots a table and it's rows.",
		RunE: func(cmd *cobra.Command, args []string) error {
			reporter := &verification.LogReporter{Printf: log.Printf}
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

			reporter.Report(verification.StatusReport{Info: "snapshot in progress"})
			if err := verification.Verify(
				ctx,
				conns,
				reporter,
				verification.WithConcurrency(flagSnapshotConcurrency),
				verification.WithTableSplits(flagSnapshotTableSplits),
				verification.WithRowBatchSize(flagSnapshotReadBatchSize),
				verification.WithWorkFunc(
					verification.Snapshot(verification.WithWriteBatchSize(flagSnapshotWriteBatchSize)),
				),
			); err != nil {
				return errors.Wrapf(err, "error snapshotting")
			}
			reporter.Report(verification.StatusReport{Info: "snapshot complete"})
			return nil
		},
	}
)

func init() {
	snapshotCmd.PersistentFlags().IntVar(
		&flagSnapshotConcurrency,
		"concurrency",
		16,
		"number of shards to process at a time",
	)
	snapshotCmd.PersistentFlags().IntVar(
		&flagSnapshotTableSplits,
		"table_splits",
		16,
		"number of ways to break down a table",
	)
	snapshotCmd.PersistentFlags().IntVar(
		&flagSnapshotReadBatchSize,
		"read_batch_size",
		20000,
		"number of rows to get from a table at a time",
	)
	snapshotCmd.PersistentFlags().IntVar(
		&flagSnapshotWriteBatchSize,
		"write_batch_size",
		10000,
		"number of rows to write at a time",
	)
	rootCmd.AddCommand(snapshotCmd)
}
