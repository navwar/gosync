// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package lfs

import (
	"context"
	"io"

	"github.com/deptofdefense/gosync/pkg/fs"

	"github.com/spf13/afero"
)

type LocalFile struct {
	file afero.File
}

func (lf *LocalFile) Close() error {
	return lf.file.Close()
}

func (lf *LocalFile) Name() string {
	return lf.file.Name()
}

func (lf *LocalFile) Read(s []byte) (int, error) {
	return lf.Read(s)
}

func (lf *LocalFile) Seek(offset int64, whence int) (int64, error) {
	return lf.Seek(offset, whence)
}

func (lf *LocalFile) Write(s []byte) (int, error) {
	return lf.Write(s)
}

func (lf *LocalFile) WriteAt(s []byte, o int64) (int, error) {
	return lf.file.WriteAt(s, o)
}

func (lf *LocalFile) WriteTo(ctx context.Context, w fs.Writer) (int64, error) {
	return io.Copy(w, lf.file)
}

func NewLocalFile(file afero.File) *LocalFile {
	return &LocalFile{
		file: file,
	}
}
