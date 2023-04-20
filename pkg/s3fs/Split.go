// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package s3fs

// Split splits the s3 path using "/"
func Split(p string) []string {
	dirs := []string{}
	d := []byte{}
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
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
