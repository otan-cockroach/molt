package cmdutil

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

type metricsConfig struct {
	listenAddr string
}

var metricsCfg = metricsConfig{
	listenAddr: "127.0.0.1:3030",
}

func RegisterMetricsFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(
		&metricsCfg.listenAddr,
		"metrics-listen-addr",
		metricsCfg.listenAddr,
		"Address for the metrics endpoint to listen to.",
	)
}

func MetricsServer(logger zerolog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := fmt.Fprint(w, "OK"); err != nil {
			logger.Err(err).Msgf("error writing to healthz")
		}
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func RunMetricsServer(logger zerolog.Logger) {
	go func() {
		m := MetricsServer(logger)
		if err := http.ListenAndServe(metricsCfg.listenAddr, m); err != nil {
			logger.Err(err).Msgf("error exposing metrics endpoints")
		}
	}()
}
