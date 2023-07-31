package datamove

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/verify/dbverify"
	"github.com/cockroachdb/molt/verify/tableverify"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2/google"
)

func Command() *cobra.Command {
	var (
		s3Bucket       string
		gcpBucket      string
		tableName      string
		source         string
		target         string
		localPath      string
		directCRDBCopy bool
		cleanup        bool
		live           bool
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
			tableName := dbtable.Name{Schema: "public", Table: tree.Name(tableName)}

			conns := dbconn.OrderedConns{source, target}

			// TODO: optimise for single table
			logger.Info().Msgf("verifying database details")
			dbTables, err := dbverify.Verify(ctx, conns)
			if err != nil {
				return err
			}
			var table dbtable.VerifiedTable
			for idx, tables := range dbTables.Verified {
				if tables[0].Name == tableName {
					r, err := tableverify.VerifyCommonTables(ctx, conns, dbTables.Verified[idx:idx+1])
					if err != nil {
						return err
					}
					if len(r[0].VerifiedTable.PrimaryKeyColumns) == 0 {
						return errors.AssertionFailedf("cannot move table as primary key do not match")
					}
					table = r[0].VerifiedTable
					for _, col := range r[0].MismatchingTableDefinitions {
						logger.Warn().
							Str("reason", col.Info).
							Msgf("not migrating %s as it mismatches", col.Name)
					}
				}
			}

			var src datamovestore.Store
			switch {
			case directCRDBCopy:
				src = datamovestore.NewCopyCRDBDirect(logger, target.(*dbconn.PGConn).Conn)
			case gcpBucket != "":
				creds, err := google.FindDefaultCredentials(ctx)
				if err != nil {
					return err
				}
				gcpClient, err := storage.NewClient(context.Background())
				if err != nil {
					return err
				}
				src = datamovestore.NewGCPStore(logger, gcpClient, creds, gcpBucket)
			case s3Bucket != "":
				sess, err := session.NewSession()
				if err != nil {
					return err
				}
				creds, err := sess.Config.Credentials.Get()
				if err != nil {
					return err
				}
				src = datamovestore.NewS3Store(logger, sess, creds, s3Bucket)
			case localPath != "":
				src, err = datamovestore.NewLocalStore(logger, localPath)
				if err != nil {
					return err
				}
			default:
				return errors.AssertionFailedf("data source must be configured (--s3-bucket, --gcp-bucket, --direct-copy)")
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
				Str("table", tableName.SafeString()).
				Str("store", fmt.Sprintf("%T", src)).
				Msg("initial config")

			logger.Info().
				Msgf("data extraction phase starting")

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

			logger.Info().
				Dur("duration", e.EndTime.Sub(e.StartTime)).
				Msgf("data extraction phase complete")

			if src.CanBeTarget() {
				if !live {
					_, err := datamove.Import(ctx, target, logger, table, e.Resources)
					if err != nil {
						return err
					}
				} else {
					_, err := datamove.Copy(ctx, target, logger, table, e.Resources)
					if err != nil {
						return err
					}
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
	cmd.PersistentFlags().BoolVar(
		&live,
		"live",
		false,
		"whether the table must be queriable during data movement",
	)
	cmd.PersistentFlags().IntVar(
		&flushSize,
		"flush-size",
		0,
		"if set, size (in bytes) before the data source is flushed",
	)
	cmd.PersistentFlags().StringVar(
		&s3Bucket,
		"s3-bucket",
		"",
		"s3 bucket",
	)
	cmd.PersistentFlags().StringVar(
		&gcpBucket,
		"gcp-bucket",
		"",
		"gcp bucket",
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

	for _, required := range []string{"source", "target"} {
		if err := cmd.MarkPersistentFlagRequired(required); err != nil {
			panic(err)
		}
	}
	return cmd
}
