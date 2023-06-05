package verification

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/rs/zerolog"
)

type Reporter interface {
	Report(obj ReportableObject)
	Close()
}

type CombinedReporter struct {
	Reporters []Reporter
}

func (c CombinedReporter) Report(obj ReportableObject) {
	for _, r := range c.Reporters {
		r.Report(obj)
	}
}

func (c CombinedReporter) Close() {
	for _, r := range c.Reporters {
		r.Close()
	}
}

// LogReporter reports to `log`.
type LogReporter struct {
	zerolog.Logger
}

func (l LogReporter) Report(obj ReportableObject) {
	switch obj := obj.(type) {
	case MissingTable:
		l.Warn().
			Str("culprit", string(obj.ConnID)).
			Str("table_schema", string(obj.Schema)).
			Str("table_name", string(obj.Table)).
			Msgf("missing table detected")
	case ExtraneousTable:
		l.Warn().
			Str("culprit", string(obj.ConnID)).
			Str("table_schema", string(obj.Schema)).
			Str("table_name", string(obj.Table)).
			Msgf("extraneous table detected")
	case MismatchingTableDefinition:
		l.Warn().
			Str("culprit", string(obj.ConnID)).
			Str("table_schema", string(obj.Schema)).
			Str("table_name", string(obj.Table)).
			Str("mismatch_info", obj.Info).
			Msgf("mismatching table definition")
	case StatusReport:
		l.Info().
			Msg(obj.Info)
	case MismatchingRow:
		falseValues := zerolog.Dict()
		truthVals := zerolog.Dict()
		for i, col := range obj.MismatchingColumns {
			// TODO: differentiate nulls
			truthVals = truthVals.Str(string(col), obj.TruthVals[i].String())
			falseValues = falseValues.Str(string(col), obj.TargetVals[i].String())
		}
		l.Warn().
			Str("culprit", string(obj.ConnID)).
			Str("table_schema", string(obj.Schema)).
			Str("table_name", string(obj.Table)).
			Dict("truth_values", truthVals).
			Dict("compare_values", falseValues).
			Strs("primary_key", zipPrimaryKeysForReporting(obj.PrimaryKeyValues)).
			Msgf("mismatching row value")
	case MissingRow:
		l.Warn().
			Str("culprit", string(obj.ConnID)).
			Str("table_schema", string(obj.Schema)).
			Str("table_name", string(obj.Table)).
			Strs("primary_key", zipPrimaryKeysForReporting(obj.PrimaryKeyValues)).
			Msgf("missing row")
	case ExtraneousRow:
		l.Warn().
			Str("culprit", string(obj.ConnID)).
			Str("table_schema", string(obj.Schema)).
			Str("table_name", string(obj.Table)).
			Strs("primary_key", zipPrimaryKeysForReporting(obj.PrimaryKeyValues)).
			Msgf("extraneous row")
	default:
		l.Error().
			Str("type", fmt.Sprintf("%T", obj)).
			Msgf("unknown object type")
	}
}

func zipPrimaryKeysForReporting(columnVals tree.Datums) []string {
	ret := make([]string, len(columnVals))
	for i := range columnVals {
		ret[i] = columnVals[i].String()
	}
	return ret
}

func (l LogReporter) Close() {
}
