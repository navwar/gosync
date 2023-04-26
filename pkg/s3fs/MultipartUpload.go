// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package s3fs

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type MultipartUpload struct {
	ctx              context.Context
	client           *s3.Client
	bucket           *string
	bucketKeyEnabled bool
	key              *string
	partSize         int
	//
	buffer         *bytes.Buffer
	uploadId       *string
	lastPartNumber int32
	etags          map[int32]*string
	closed         bool
}

func (mu *MultipartUpload) Close() error {
	if mu.closed {
		return io.ErrUnexpectedEOF
	}

	mu.closed = true

	if mu.uploadId == nil {
		return nil
	}

	// upload remaining bytes
	if mu.buffer.Len() > 0 {
		partNumber := mu.lastPartNumber + 1
		uploadPartOutput, err := mu.client.UploadPart(mu.ctx, &s3.UploadPartInput{
			Body:          mu.buffer, // pass in buffer
			Bucket:        mu.bucket,
			Key:           mu.key,
			PartNumber:    partNumber,
			UploadId:      mu.uploadId,
			ContentLength: int64(mu.buffer.Len()),
		})
		if err != nil {
			return err
		}

		// save etag
		mu.etags[partNumber] = uploadPartOutput.ETag

		// incremeent part number
		mu.lastPartNumber = partNumber

		// reset buffer
		mu.buffer = bytes.NewBuffer([]byte{})
	}

	// build list of completed parts
	completedParts := []types.CompletedPart{}
	for i := int32(1); i <= mu.lastPartNumber; i++ {
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       mu.etags[i],
			PartNumber: i,
		})
	}

	// complete multipart upload
	_, err := mu.client.CompleteMultipartUpload(mu.ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   mu.bucket,
		Key:      mu.key,
		UploadId: mu.uploadId,
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

func (mu *MultipartUpload) Write(p []byte) (int, error) {
	if mu.closed {
		return 0, io.ErrUnexpectedEOF
	}

	if mu.uploadId == nil {
		createMultipartUploadOutput, err := mu.client.CreateMultipartUpload(mu.ctx, &s3.CreateMultipartUploadInput{
			ACL:              types.ObjectCannedACLBucketOwnerFullControl,
			Bucket:           mu.bucket,
			BucketKeyEnabled: mu.bucketKeyEnabled,
			// ContentType: aws.String(fileType),
			Key: mu.key,
		})
		if err != nil {
			return 0, err
		}
		mu.bucket = createMultipartUploadOutput.Bucket
		mu.key = createMultipartUploadOutput.Key
		mu.uploadId = createMultipartUploadOutput.UploadId
		mu.buffer = bytes.NewBuffer([]byte{})
	}

	// write to internal buffer
	n, err := mu.buffer.Write(p)
	if err != nil {
		return 0, err
	}

	// check if buffer is greater than part size
	if mu.buffer.Len() >= mu.partSize {
		partNumber := mu.lastPartNumber + 1
		uploadPartOutput, err := mu.client.UploadPart(mu.ctx, &s3.UploadPartInput{
			Body:          mu.buffer, // pass in buffer
			Bucket:        mu.bucket,
			Key:           mu.key,
			PartNumber:    partNumber,
			UploadId:      mu.uploadId,
			ContentLength: int64(mu.buffer.Len()),
		})
		if err != nil {
			return 0, err
		}

		// save etag
		mu.etags[partNumber] = uploadPartOutput.ETag

		// incremeent part number
		mu.lastPartNumber = partNumber

		// reset buffer
		mu.buffer = bytes.NewBuffer([]byte{})
	}

	// return number of bytes written
	return n, nil
}

func NewMultipartUpload(ctx context.Context, client *s3.Client, bucket string, bucketKeyEnabled bool, key string, partSize int) *MultipartUpload {
	return &MultipartUpload{
		client:           client,
		ctx:              ctx,
		bucket:           aws.String(bucket),
		bucketKeyEnabled: bucketKeyEnabled,
		key:              aws.String(key),
		partSize:         partSize,
		//
		buffer:         nil,
		uploadId:       nil,
		lastPartNumber: int32(0),
		etags:          map[int32]*string{},
		closed:         false,
	}
}
