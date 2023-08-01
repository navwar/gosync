// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package fs

type Logger interface {
	Log(msg string, fields ...map[string]interface{}) error
}
