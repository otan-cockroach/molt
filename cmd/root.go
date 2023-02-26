package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "molt",
	Short: "Onboarding assistance for migrating to CockroachDB",
	Long:  `MOLT (Migrate Off Legacy Things) provides tooling which assists migrating off other database providers to CockroachDB.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
