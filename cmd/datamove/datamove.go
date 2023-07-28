package datamove

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	var (
		bucket         string
		tableName      string
		source         string
		target         string
		localPath      string
		directCRDBCopy bool
		cleanup        bool
		flushSize      int
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

			var src datamovestore.Store
			switch {
			case directCRDBCopy:
				src = datamovestore.NewCopyCRDBDirect(logger, target.(*dbconn.PGConn).Conn)
			case bucket != "":
				sess := session.Must(session.NewSession())
				src = datamovestore.NewS3Store(logger, sess, bucket)
			case localPath != "":
				src, err = datamovestore.NewLocalStore(logger, localPath)
				if err != nil {
					return err
				}
			default:
				return errors.AssertionFailedf("data source must be configured (--s3-bucket, --direct-copy)")
			}
			if flushSize == 0 {
				flushSize = src.DefaultFlushBatchSize()
			}
			defer func() {
				if cleanup {
					if err := src.Cleanup(ctx); err != nil {
						logger.Err(err).Msgf("error marking object for cleanup")
					}
				}
			}()
			logger.Debug().
				Int("flush_size", flushSize).
				Str("table", table.SafeString()).
				Str("store", fmt.Sprintf("%T", src)).
				Msg("initial config")

			startTime := time.Now()
			e, err := datamove.Export(ctx, source, logger, src, table, flushSize)
			if err != nil {
				return err
			}
			defer func() {
				if cleanup {
					for _, r := range e.Resources {
						if err := r.MarkForCleanup(ctx); err != nil {
							logger.Err(err).Msgf("error cleaning up resource")
						}
					}
				}
			}()
			if src.CanBeTarget() {
				_, err := datamove.Import(ctx, target, logger, table, e.Resources)
				if err != nil {
					return err
				}
			}

			logger.Info().
				Dur("duration", time.Since(startTime)).
				Str("snapshot_id", e.SnapshotID).
				Msg("data movement complete")

			return nil
		},
	}

	cmd.PersistentFlags().BoolVar(
		&directCRDBCopy,
		"direct-copy",
		false,
		"whether to use direct copy mode",
	)

	cmd.PersistentFlags().BoolVar(
		&cleanup,
		"cleanup",
		false,
		"whether any file resources created should be deleted",
	)
	cmd.PersistentFlags().IntVar(
		&flushSize,
		"flush-size",
		0,
		"if set, size (in bytes) before the data source is flushed",
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
		"URL of the target database",
	)
	cmd.PersistentFlags().StringVar(
		&localPath,
		"local-path",
		"",
		"path to upload files to locally",
	)
	return cmd
}
