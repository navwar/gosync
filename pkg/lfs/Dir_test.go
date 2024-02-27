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

func TestDir(t *testing.T) {
	assert.Equal(t, "a", Dir("a/b"))
	assert.Equal(t, "a", Dir("a/b/"))
	assert.Equal(t, "a/b", Dir("a/b/c"))
	assert.Equal(t, "a/b", Dir("a/b/cd"))
	assert.Equal(t, "/a", Dir("/a/b/"))
	assert.Equal(t, "/a/b", Dir("/a/b/c/"))
	assert.Equal(t, "/a/b", Dir("/a/b/cd/"))
}
