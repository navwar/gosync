// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package s3fs

import (
	"path"
)

// Dir returns all but the last element of path
func Dir(p string) string {
	if len(p) == 0 {
		return "."
	}
	directories := Split(p)
	if len(directories) == 1 {
		if directories[0] == "/" {
			return "/"
		}
		return "."
	}
	return path.Join(directories[0 : len(directories)-1]...)
}
