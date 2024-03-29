// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package lfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollectFiles(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, Split("a/b"))
	assert.Equal(t, []string{"a", "b"}, Split("a/b/"))
	assert.Equal(t, []string{"/", "a", "b"}, Split("/a/b/"))
}
