// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

import (
	"io"
)

type File interface {
	io.ReadSeekCloser
	io.WriteCloser
	Name() string
}
