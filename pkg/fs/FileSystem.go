// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

import (
	"context"
	"os"
)

type FileSystem interface {
	Copy(ctx context.Context, source string, destination string, parents bool, logger Logger) error
	Dir(name string) string
	IsNotExist(err error) bool
	Join(name ...string) string
	MkdirAll(ctx context.Context, name string, mode os.FileMode) (err error)
	Open(ctx context.Context, name string) (File, error)
	OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (File, error)
	ReadDir(ctx context.Context, name string) ([]DirectoryEntry, error)
	Size(ctx context.Context, name string) (int64, error)
	Stat(ctx context.Context, name string) (FileInfo, error)
	Sync(ctx context.Context, source string, destination string, parents bool, checkTimestamps bool, limit int, logger Logger) (int, error)
	SyncDirectory(ctx context.Context, source string, destinationDirectory string, checkTimestamps bool, limit int, logger Logger) (int, error)
}
