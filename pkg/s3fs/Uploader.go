// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package s3fs

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Uploader struct {
	ctx context.Context
	//
	acl              types.ObjectCannedACL
	client           *s3.Client
	bucket           *string
	bucketKeyEnabled bool
	key              *string
	partSize         int
	//
	buffer         *bytes.Buffer
	uploadID       *string
	lastPartNumber int32
	etags          map[int32]*string
	closed         bool
}

func (u *Uploader) Close() error {
	if u.closed {
		return io.ErrUnexpectedEOF
	}

	u.closed = true

	// if upload hasn't started.
	if u.uploadID == nil {
		// create reader
		// a readseeker inferface is needed to rewind
		// the reader to the beginning if the first attempt failed
		// and the client retries
		reader := bytes.NewReader(u.buffer.Bytes())
		// put the object
		_, err := u.client.PutObject(u.ctx, &s3.PutObjectInput{
			ACL:              u.acl,
			Body:             reader,
			Bucket:           u.bucket,
			BucketKeyEnabled: u.bucketKeyEnabled,
			ContentLength:    int64(reader.Len()),
			Key:              u.key,
		})
		if err != nil {
			return err
		}
		// reset buffer
		u.buffer = bytes.NewBuffer([]byte{})
		// return
		return nil
	}

	// upload remaining bytes
	if u.buffer.Len() > 0 {
		// create reader
		// a readseeker inferface is needed to rewind
		// the reader to the beginning if the first attempt failed
		// and the client retries
		reader := bytes.NewReader(u.buffer.Bytes())
		// create part number
		partNumber := u.lastPartNumber + 1
		// upload part
		uploadPartOutput, err := u.client.UploadPart(u.ctx, &s3.UploadPartInput{
			Body:          reader,
			Bucket:        u.bucket,
			Key:           u.key,
			PartNumber:    partNumber,
			UploadId:      u.uploadID,
			ContentLength: int64(reader.Len()),
		})
		if err != nil {
			return err
		}

		// save etag
		u.etags[partNumber] = uploadPartOutput.ETag

		// increment part number
		u.lastPartNumber = partNumber

		// reset buffer
		u.buffer = bytes.NewBuffer([]byte{})
	}

	// build list of completed parts
	completedParts := []types.CompletedPart{}
	for i := int32(1); i <= u.lastPartNumber; i++ {
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       u.etags[i],
			PartNumber: i,
		})
	}

	// complete multipart upload
	_, err := u.client.CompleteMultipartUpload(u.ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   u.bucket,
		Key:      u.key,
		UploadId: u.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return err
	}
	// return
	return nil
}

func (u *Uploader) Write(p []byte) (int, error) {
	if u.closed {
		return 0, io.ErrUnexpectedEOF
	}

	// write to internal buffer
	n, err := u.buffer.Write(p)
	if err != nil {
		return 0, err
	}

	// check if buffer is greater than part size
	if u.buffer.Len() >= u.partSize {

		// If multipart upload hasn't been started yet, then create it.
		if u.uploadID == nil {
			createMultipartUploadOutput, err := u.client.CreateMultipartUpload(u.ctx, &s3.CreateMultipartUploadInput{
				ACL:              types.ObjectCannedACLBucketOwnerFullControl,
				Bucket:           u.bucket,
				BucketKeyEnabled: u.bucketKeyEnabled,
				Key:              u.key,
			})
			if err != nil {
				return 0, err
			}
			u.uploadID = createMultipartUploadOutput.UploadId
		}

		// Upload Part
		partNumber := u.lastPartNumber + 1
		uploadPartOutput, err := u.client.UploadPart(u.ctx, &s3.UploadPartInput{
			Body:          u.buffer, // pass in buffer
			Bucket:        u.bucket,
			Key:           u.key,
			PartNumber:    partNumber,
			UploadId:      u.uploadID,
			ContentLength: int64(u.buffer.Len()),
		})
		if err != nil {
			return 0, err
		}

		// save etag
		u.etags[partNumber] = uploadPartOutput.ETag

		// incremeent part number
		u.lastPartNumber = partNumber

		// reset buffer
		u.buffer = bytes.NewBuffer([]byte{})
	}

	// return number of bytes written
	return n, nil
}

type UploaderInput struct {
	ACL              types.ObjectCannedACL
	Client           *s3.Client
	Bucket           string
	BucketKeyEnabled bool
	Key              string
	PartSize         int
}

func NewUploader(ctx context.Context, input *UploaderInput) *Uploader {
	return &Uploader{
		ctx: ctx,
		//
		acl:              input.ACL,
		client:           input.Client,
		bucket:           aws.String(input.Bucket),
		bucketKeyEnabled: input.BucketKeyEnabled,
		key:              aws.String(input.Key),
		partSize:         input.PartSize,
		//
		buffer:         bytes.NewBuffer([]byte{}),
		uploadID:       nil,
		lastPartNumber: int32(0),
		etags:          map[int32]*string{},
		closed:         false,
	}
}
