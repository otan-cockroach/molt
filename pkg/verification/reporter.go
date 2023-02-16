package verification

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
	default:
		l.Printf("[ERROR] unable to process %#v", obj)
	}
}

func (l LogReporter) Close() {
}
