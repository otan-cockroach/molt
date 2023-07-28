package datamove

import "io"

type csvPipe struct {
	in  io.Reader
	out io.WriteCloser

	scratch []byte
	buf     []byte

	flushSize int
	currSize  int
	numRows   int
	newWriter func() io.WriteCloser
}

const defaultBufSize = 1024 * 1024

func newCSVPipe(in io.Reader, flushSize int, newWriter func() io.WriteCloser) *csvPipe {
	return &csvPipe{
		in:        in,
		flushSize: flushSize,
		newWriter: newWriter,

		scratch: make([]byte, defaultBufSize),
		buf:     make([]byte, 0, defaultBufSize),
	}
}

func (p *csvPipe) Pipe() error {
	p.out = p.newWriter()

	for {
		n, err := p.in.Read(p.scratch)
		if err != nil {
			if err == io.EOF {
				_, err = p.out.Write(p.buf)
				if err != nil {
					return err
				}
				return p.out.Close()
			}
			return err
		}

		p.buf = append(p.buf, p.scratch[:n]...)
		numItems, lineStart := lastNewLineInCSV(p.buf)
		p.numRows += numItems
		if _, err := p.out.Write(p.buf[:lineStart]); err != nil {
			return err
		}
		p.currSize += lineStart
		if p.currSize > p.flushSize {
			if err := p.out.Close(); err != nil {
				return err
			}
			p.currSize = 0
			p.out = p.newWriter()
		}
		copy(p.buf, p.buf[lineStart:])
		p.buf = p.buf[:len(p.buf)-lineStart]
	}
}

func lastNewLineInCSV(buf []byte) (numItems, lastNewLineIdx int) {
	curr := 0
	quoted := false
	for curr < len(buf) {
		lookaheadIs := func(r byte) bool {
			return curr < len(buf)-1 && buf[curr+1] == r
		}

		// In quotes.
		if buf[curr] == '"' {
			if quoted && lookaheadIs('"') {
				curr += 2
				continue
			}
			quoted = !quoted
			curr++
			continue
		}
		if quoted {
			curr++
			continue
		}
		// Escapes.
		if buf[curr] == '\\' {
			curr += 2
			continue
		}
		if buf[curr] == '\n' {
			numItems++
			lastNewLineIdx = curr + 1
		}
		curr++
	}
	return numItems, lastNewLineIdx
}
