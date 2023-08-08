package datamove

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/cmd/internal/cmdutil"
	"github.com/cockroachdb/molt/datamove"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2/google"
)

func Command() *cobra.Command {
	var (
		s3Bucket                string
		gcpBucket               string
		localPath               string
		localPathListenAddr     string
		localPathCRDBAccessAddr string
		directCRDBCopy          bool
		cfg                     datamove.Config
	)
	cmd := &cobra.Command{
		Use:  "datamove",
		Long: `Moves data from a source to a target.`,

		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			logger, err := cmdutil.Logger()
			if err != nil {
				return err
			}
			cmdutil.RunMetricsServer(logger)

			conns, err := cmdutil.LoadDBConns(ctx)
			if err != nil {
				return err
			}
			if pgx, ok := conns[1].(*dbconn.PGConn); !ok || !pgx.IsCockroach() {
				return errors.AssertionFailedf("target must be cockroach")
			}

			var src datamovestore.Store
			switch {
			case directCRDBCopy:
				src = datamovestore.NewCopyCRDBDirect(logger, conns[1].(*dbconn.PGConn).Conn)
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
				src, err = datamovestore.NewLocalStore(logger, localPath, localPathListenAddr, localPathCRDBAccessAddr)
				if err != nil {
					return err
				}
			default:
				return errors.AssertionFailedf("data source must be configured (--s3-bucket, --gcp-bucket, --direct-copy)")
			}
			return datamove.DataMove(
				ctx,
				cfg,
				logger,
				conns,
				src,
				cmdutil.TableFilter(),
			)
		},
	}

	cmd.PersistentFlags().BoolVar(
		&directCRDBCopy,
		"direct-copy",
		false,
		"whether to use direct copy mode",
	)
	cmd.PersistentFlags().BoolVar(
		&cfg.Cleanup,
		"cleanup",
		false,
		"whether any file resources created should be deleted",
	)
	cmd.PersistentFlags().BoolVar(
		&cfg.Live,
		"live",
		false,
		"whether the table must be queriable during data movement",
	)
	cmd.PersistentFlags().IntVar(
		&cfg.FlushSize,
		"flush-size",
		0,
		"if set, size (in bytes) before the data source is flushed",
	)
	cmd.PersistentFlags().IntVar(
		&cfg.Concurrency,
		"concurrency",
		4,
		"number of tables to move data with at a time",
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
		&localPath,
		"local-path",
		"",
		"path to upload files to locally",
	)
	cmd.PersistentFlags().StringVar(
		&localPathListenAddr,
		"local-path-listen-addr",
		"",
		"local address to listen to for traffic",
	)
	cmd.PersistentFlags().StringVar(
		&localPathCRDBAccessAddr,
		"local-path-crdb-access-addr",
		"",
		"address CockroachDB can access to connect to the --local-path-listen-addr",
	)
	cmd.PersistentFlags().BoolVar(
		&cfg.Truncate,
		"truncate",
		false,
		"whether to truncate the table being imported to",
	)
	cmdutil.RegisterDBConnFlags(cmd)
	cmdutil.RegisterLoggerFlags(cmd)
	cmdutil.RegisterNameFilterFlags(cmd)
	cmdutil.RegisterMetricsFlags(cmd)
	return cmd
}
