// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

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
	MagicNumber(ctx context.Context, name string) ([]byte, error)
	MagicNumbers(ctx context.Context, names []string, threads int) ([][]byte, error)
	MkdirAll(ctx context.Context, name string, mode os.FileMode) error
	MustRelative(ctx context.Context, basepath string, targetpath string) string
	Open(ctx context.Context, name string) (File, error)
	OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (File, error)
	Relative(ctx context.Context, basepath string, targetpath string) (string, error)
	ReadDir(ctx context.Context, name string, recursive bool) ([]DirectoryEntryInterface, error)
	ReadFile(ctx context.Context, name string, p []byte) (n int, err error) // open up a file and read the contents
	RemoveFile(ctx context.Context, name string) error
	RemoveFiles(ctx context.Context, names []string) error
	RemoveDirectory(ctx context.Context, name string, recursive bool) error
	RemoveDirectories(ctx context.Context, name []string, recursive bool) error
	Root() string
	Size(ctx context.Context, name string) (int64, error)
	Stat(ctx context.Context, name string) (FileInfo, error)
	Sync(ctx context.Context, input *SyncInput) (int, error)
	SyncDirectory(ctx context.Context, input *SyncDirectoryInput) (int, error)
}
