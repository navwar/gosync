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
	"time"
)

type FileSystem interface {
	Chtimes(ctx context.Context, name string, atime time.Time, mtime time.Time) error
	Copy(ctx context.Context, input *CopyInput) error
	Dir(name string) string
	IsNotExist(err error) bool
	Join(name ...string) string
	MkdirAll(ctx context.Context, name string, mode os.FileMode) error
	Open(ctx context.Context, name string) (File, error)
	OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (File, error)
	ReadDir(ctx context.Context, name string) ([]DirectoryEntry, error)
	Root() string
	Size(ctx context.Context, name string) (int64, error)
	Stat(ctx context.Context, name string) (FileInfo, error)
	Sync(ctx context.Context, input *SyncInput) (int, error)
	SyncDirectory(ctx context.Context, input *SyncDirectoryInput) (int, error)
}
