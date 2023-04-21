// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

type Logger interface {
	Log(msg string, fields ...map[string]interface{}) error
}
