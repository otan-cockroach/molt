package datamove

import (
	"encoding/csv"
	"io"
)

type csvPipe struct {
	in io.Reader

	csvWriter *csv.Writer
	out       io.WriteCloser

	flushSize int
	currSize  int
	numRows   int
	newWriter func() io.WriteCloser
}

func newCSVPipe(in io.Reader, flushSize int, newWriter func() io.WriteCloser) *csvPipe {
	return &csvPipe{
		in:        in,
		flushSize: flushSize,
		newWriter: newWriter,
	}
}

func (p *csvPipe) Pipe() error {
	if err := p.flush(true); err != nil {
		return err
	}
	r := csv.NewReader(p.in)
	r.ReuseRecord = true
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return p.flush(false)
			}
			return err
		}
		p.numRows++
		for _, s := range record {
			p.currSize += len(s) + 1
		}
		if err := p.csvWriter.Write(record); err != nil {
			return err
		}
		if p.currSize > p.flushSize {
			if err := p.flush(true); err != nil {
				return err
			}
		}
	}
}

func (p *csvPipe) flush(recreate bool) error {
	if p.csvWriter != nil {
		p.csvWriter.Flush()
		if err := p.out.Close(); err != nil {
			return err
		}
	}
	p.currSize = 0
	if recreate {
		p.out = p.newWriter()
		p.csvWriter = csv.NewWriter(p.out)
	} else {
		p.out = nil
		p.csvWriter = nil
	}
	return nil
}
