package datamovestore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
)

type localStore struct {
	logger     zerolog.Logger
	basePath   string
	cleanPaths map[string]struct{}
}

func NewLocalStore(logger zerolog.Logger, basePath string) (*localStore, error) {
	return &localStore{
		logger:     logger,
		basePath:   basePath,
		cleanPaths: make(map[string]struct{}),
	}, nil
}

func (l *localStore) CreateFromReader(
	ctx context.Context, r io.Reader, table dbtable.Name, iteration int,
) (Resource, error) {
	baseDir := path.Join(l.basePath, table.SafeString())
	if iteration == 1 {
		l.cleanPaths[baseDir] = struct{}{}
		if err := os.MkdirAll(baseDir, os.ModePerm); err != nil {
			return nil, err
		}
	}
	fileName := path.Join(baseDir, fmt.Sprintf("part_%08d.csv", iteration))
	logger := l.logger.With().Str("path", fileName).Logger()
	logger.Debug().Msgf("creating file")
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 1024*1024)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				logger.Debug().Msgf("wrote file")
				return &localResource{path: fileName}, nil
			}
			return nil, err
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return nil, err
		}
	}
}

func (l *localStore) DefaultFlushBatchSize() int {
	return 128 * 1024 * 1024
}

func (l *localStore) Cleanup(ctx context.Context) error {
	for path := range l.cleanPaths {
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func (l *localStore) CanBeTarget() bool {
	return true
}

type localResource struct {
	path string
}

func (l *localResource) Reader(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(l.path)
}

func (l *localResource) ImportURL() (string, error) {
	return "", errors.AssertionFailedf("cannot IMPORT from a local path")
}

func (l *localResource) MarkForCleanup(ctx context.Context) error {
	return os.Remove(l.path)
}
