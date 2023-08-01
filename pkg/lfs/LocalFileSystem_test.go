// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package lfs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalFileSystemRelative(t *testing.T) {
	ctx := context.Background()
	lfs := &LocalFileSystem{}

	base := "/a"

	relpath, err := lfs.Relative(ctx, base, "/a")
	assert.NoError(t, err)
	assert.Equal(t, ".", relpath)

	relpath, err = lfs.Relative(ctx, base, "/a/b/c")
	assert.NoError(t, err)
	assert.Equal(t, "b/c", relpath)

	relpath, err = lfs.Relative(ctx, base, "/b/c")
	assert.NoError(t, err)
	assert.Equal(t, "../b/c", relpath)

	relpath, err = lfs.Relative(ctx, base, "./b/c")
	assert.Error(t, err)
	assert.Equal(t, "Rel: can't make ./b/c relative to "+base, err.Error())
	assert.Equal(t, "", relpath)
}
