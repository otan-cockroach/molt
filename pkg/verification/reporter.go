package verification

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
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
	Printf func(fmt string, args ...any)
}

func (l LogReporter) Report(obj ReportableObject) {
	switch obj := obj.(type) {
	case MissingTable:
		l.Printf("[DATABASE MISMATCH] %s is missing table %s.%s", obj.ConnID, obj.Schema, obj.Table)
	case ExtraneousTable:
		l.Printf("[DATABASE MISMATCH] %s has an extraneous table %s.%s", obj.ConnID, obj.Schema, obj.Table)
	case MismatchingTableDefinition:
		l.Printf("[TABLE MISMATCH] table %s.%s on %s has an issue: %s", obj.Schema, obj.Table, obj.ConnID, obj.Info)
	case StatusReport:
		l.Printf("[STATUS] %s", obj.Info)
	case MismatchingRow:
		f := fmt.Sprintf("[ROW MISMATCH] table %s.%s on %s has a mismatching row on (%s): ", obj.Schema, obj.Table, obj.ConnID, zipPrimaryKeysForReporting(obj.PrimaryKeyColumns, obj.PrimaryKeyValues))
		for i, col := range obj.MismatchingColumns {
			if i > 0 {
				f += ", "
			}
			f += fmt.Sprintf("column %s: %s vs %s", col, obj.TargetVals[i].String(), obj.TruthVals[i].String())
		}
		l.Printf(f)
	case MissingRow:
		l.Printf("[ROW MISMATCH] table %s.%s on %s is missing a row with PK (%s)", obj.Schema, obj.Table, obj.ConnID, zipPrimaryKeysForReporting(obj.PrimaryKeyColumns, obj.PrimaryKeyValues))
	case ExtraneousRow:
		l.Printf("[ROW MISMATCH] table %s.%s on %s has an extraneous row with PK (%s)", obj.Schema, obj.Table, obj.ConnID, zipPrimaryKeysForReporting(obj.PrimaryKeyColumns, obj.PrimaryKeyValues))
	default:
		l.Printf("[ERROR] unable to process %#v", obj)
	}
}

func zipPrimaryKeysForReporting(columnNames []tree.Name, columnVals tree.Datums) string {
	var sb strings.Builder
	for i := range columnNames {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("%s=%s", columnNames[i], columnVals[i].String()))
	}
	return sb.String()
}

func (l LogReporter) Close() {
}
