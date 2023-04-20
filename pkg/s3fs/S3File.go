// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package s3fs

import (
	"io"
)

type S3File struct {
	name        string
	readSeeker  io.ReadSeeker
	writeCloser io.WriteCloser
}

func (f *S3File) Name() string {
	return f.name
}

func (f *S3File) Close() error {
	if f.writeCloser != nil {
		return f.writeCloser.Close()
	}
	return nil
}

func (f *S3File) Read(p []byte) (int, error) {
	return f.readSeeker.Read(p)
}

func (f *S3File) Seek(offset int64, whence int) (int64, error) {
	return f.readSeeker.Seek(offset, whence)
}

func (f *S3File) Write(p []byte) (n int, err error) {
	if f.writeCloser != nil {
		return f.writeCloser.Write(p)
	}
	return 0, nil
}

func NewS3File(name string, readSeeker io.ReadSeeker, writeCloser io.WriteCloser) *S3File {
	return &S3File{
		name:        name,
		readSeeker:  readSeeker,
		writeCloser: writeCloser,
	}
}
