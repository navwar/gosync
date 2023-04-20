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
	//
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

	completedParts := []types.CompletedPart{}

	for i := int32(1); i <= mu.lastPartNumber; i++ {
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       mu.etags[i],
			PartNumber: i,
		})
	}

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
	}

	partNumber := mu.lastPartNumber + 1

	uploadPartOutput, err := mu.client.UploadPart(mu.ctx, &s3.UploadPartInput{
		Body:          bytes.NewReader(p),
		Bucket:        mu.bucket,
		Key:           mu.key,
		PartNumber:    partNumber,
		UploadId:      mu.uploadId,
		ContentLength: int64(len(p)),
	})
	if err != nil {
		return 0, err
	}

	// save etag
	mu.etags[partNumber] = uploadPartOutput.ETag

	// incremeent part number
	mu.lastPartNumber = partNumber

	// return number of bytes written
	return len(p), nil
}

func NewMultipartUpload(ctx context.Context, client *s3.Client, bucket string, bucketKeyEnabled bool, key string) *MultipartUpload {
	return &MultipartUpload{
		client:           client,
		ctx:              ctx,
		bucket:           aws.String(bucket),
		bucketKeyEnabled: bucketKeyEnabled,
		key:              aws.String(key),
		//
		uploadId:       nil,
		lastPartNumber: int32(0),
		etags:          map[int32]*string{},
		closed:         false,
	}
}
