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

func TestCheck(t *testing.T) {
	assert.NoError(t, Check("a", "b"))
	assert.NoError(t, Check("a/c", "b/c"))
	assert.NoError(t, Check("a/c/d", "a/c/e"))
	assert.Error(t, Check("a/b", "a"))
	assert.Error(t, Check("a", "a/b"))
	assert.Error(t, Check("a", "a/b/c"))
	assert.Error(t, Check("a/b", "a/b/c"))
	assert.Error(t, Check("a/b/c", "a/b"))
	assert.Error(t, Check("a/b/c", "a"))
}
