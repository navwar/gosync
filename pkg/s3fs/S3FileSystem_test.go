// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package s3fs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3FileSystemRelative(t *testing.T) {
	ctx := context.Background()
	s3fs := &S3FileSystem{}

	base := "/a"

	relpath, err := s3fs.Relative(ctx, base, "/a")
	assert.NoError(t, err)
	assert.Equal(t, ".", relpath)

	relpath, err = s3fs.Relative(ctx, base, "/a/b/c")
	assert.NoError(t, err)
	assert.Equal(t, "b/c", relpath)

	relpath, err = s3fs.Relative(ctx, base, "/b/c")
	assert.NoError(t, err)
	assert.Equal(t, "../b/c", relpath)

	relpath, err = s3fs.Relative(ctx, base, "./b/c")
	assert.Error(t, err)
	assert.Equal(t, "Rel: can't make ./b/c relative to "+base, err.Error())
	assert.Equal(t, "", relpath)
}
