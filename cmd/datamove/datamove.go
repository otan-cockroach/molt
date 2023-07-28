package datamove

import (
	"context"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/datamove"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	var (
		bucket     string
		tableName  string
		source     string
		target     string
		useSQLCopy bool
	)
	cmd := &cobra.Command{
		Use:  "datamove",
		Long: `Moves data from a source to a target.`,

		RunE: func(cmd *cobra.Command, args []string) error {
			cw := zerolog.NewConsoleWriter()
			logger := zerolog.New(cw)

			ctx := context.Background()

			source, err := dbconn.Connect(ctx, "source", source)
			if err != nil {
				return err
			}
			target, err := dbconn.Connect(ctx, "target", target)
			if err != nil {
				return err
			}
			table := dbtable.Name{Schema: "public", Table: tree.Name(tableName)}
			var copyConn dbconn.Conn
			if useSQLCopy {
				copyConn = target
			}
			e, err := datamove.Export(ctx, source, logger, bucket, table, copyConn)
			if err != nil {
				return err
			}
			if !useSQLCopy {
				_, err := datamove.Import(ctx, target, logger, bucket, table, e.Files)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.PersistentFlags().BoolVar(
		&useSQLCopy,
		"copy",
		false,
		"whether to use useSQLCopy mode instead",
	)
	cmd.PersistentFlags().StringVar(
		&bucket,
		"s3-bucket",
		"",
		"s3 bucket",
	)
	cmd.PersistentFlags().StringVar(
		&tableName,
		"table",
		"",
		"table to migrate",
	)
	cmd.PersistentFlags().StringVar(
		&source,
		"source",
		"",
		"URL of the source database",
	)
	cmd.PersistentFlags().StringVar(
		&target,
		"target",
		"",
		"URL of the source database",
	)
	return cmd
}
