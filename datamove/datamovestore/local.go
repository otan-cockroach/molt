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
	logger   zerolog.Logger
	basePath string
}

func NewLocalStore(logger zerolog.Logger, basePath string) (*localStore, error) {
	return &localStore{
		logger:   logger,
		basePath: basePath,
	}, nil
}

func (l *localStore) CreateFromReader(
	ctx context.Context, r io.Reader, table dbtable.Name, iteration int,
) (Resource, error) {
	baseDir := path.Join(l.basePath, table.SafeString())
	if iteration == 1 {
		if _, err := os.Stat(baseDir); err == nil {
			l.logger.Debug().
				Str("dir", baseDir).
				Msg("removing existing directory")
			if err := os.RemoveAll(baseDir); err != nil {
				return nil, err
			}
		}
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
	return os.RemoveAll(l.basePath)
}

func (l *localStore) CanBeTarget() bool {
	return false
}

type localResource struct {
	path string
}

func (l localResource) ImportURL() (string, error) {
	return "", errors.AssertionFailedf("cannot IMPORT from a local path")
}

func (l localResource) MarkForCleanup(ctx context.Context) error {
	return nil
}
