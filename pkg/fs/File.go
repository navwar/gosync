// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package fs

import (
	"context"
	"io"
)

type File interface {
	io.ReadSeekCloser
	Writer
	Name() string
	WriteTo(ctx context.Context, w Writer) (int64, error)
}
