// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package lfs

import (
	"os"
)

// Split splits the path using the path separator for the local operating system
func Split(p string) []string {
	dirs := []string{}
	d := []byte{}
	for i := 0; i < len(p); i++ {
		if os.IsPathSeparator(p[i]) {
			if len(d) == 0 {
				dirs = append(dirs, "/")
			} else {
				dirs = append(dirs, string(d))
			}
			d = []byte{}
			continue
		}
		d = append(d, p[i])
	}
	if len(d) > 0 {
		if len(d) == 0 {
			dirs = append(dirs, "/")
		} else {
			dirs = append(dirs, string(d))
		}
	}
	return dirs
}
