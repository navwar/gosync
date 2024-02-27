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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/navwar/gosync/pkg/fs"
)

type S3FileSystem struct {
	defaultRegion        string
	bucket               string
	prefix               string
	clients              map[string]*s3.Client
	bucketRegions        map[string]string
	bucketCreationDates  map[string]time.Time
	earliestCreationDate time.Time
	maxEntries           int
	maxPages             int
	bucketKeyEnabled     bool
	partSize             int64
}

func (s3fs *S3FileSystem) Chtimes(ctx context.Context, name string, atime time.Time, mtime time.Time) error {
	return nil
}

// CopyObject uses the CopyObject API to copy an object.
func (s3fs *S3FileSystem) CopyObject(ctx context.Context, source string, destination string) error {
	sourceBucket, sourceKey := s3fs.parse(source)
	destinationBucket, destinationKey := s3fs.parse(destination)
	_, err := s3fs.clients[s3fs.GetBucketRegion(destinationBucket)].CopyObject(ctx, &s3.CopyObjectInput{
		ACL:              types.ObjectCannedACLBucketOwnerFullControl,
		Bucket:           aws.String(destinationBucket),
		BucketKeyEnabled: aws.Bool(s3fs.bucketKeyEnabled),
		Key:              aws.String(destinationKey),
		CopySource:       aws.String(fmt.Sprintf("%s/%s", sourceBucket, sourceKey)),
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteObject uses the DeleteObject API to delete an object.
func (s3fs *S3FileSystem) DeleteObject(ctx context.Context, name string) error {
	bucket, key := s3fs.parse(name)
	client := s3fs.clients[s3fs.GetBucketRegion(bucket)]
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteObjects uses the DeleteObjects API to delete multiple objects.
func (s3fs *S3FileSystem) DeleteObjects(ctx context.Context, names []string) error {
	objectsToDeleteByBucket := map[string][]string{}
	for _, name := range names {
		bucket, key := s3fs.parse(name)
		if keys, ok := objectsToDeleteByBucket[bucket]; ok {
			objectsToDeleteByBucket[bucket] = append(keys, key)
		} else {
			objectsToDeleteByBucket[bucket] = []string{key}
		}
	}
	for bucket, keys := range objectsToDeleteByBucket {
		client := s3fs.clients[s3fs.GetBucketRegion(bucket)]
		for i := 0; i < len(keys); i += 1000 {
			objects := make([]types.ObjectIdentifier, 0)
			for j := 0; (j < 1000) && (i+j < len(keys)); j++ {
				objects = append(objects, types.ObjectIdentifier{
					Key: aws.String(keys[i+j]),
				})
			}
			delete := &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			}
			_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: delete,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// MultipartCopyObject copies an object using a multipart upload, which is required for objects greater than or equal to 5 GB.
func (s3fs *S3FileSystem) MultipartCopyObject(ctx context.Context, source string, destination string, size int64) error {
	// parse source
	sourceBucket, sourceKey := s3fs.parse(source)

	// parse destination
	destinationBucket, destinationKey := s3fs.parse(destination)

	// get client
	client := s3fs.clients[s3fs.GetBucketRegion(destinationBucket)]

	// create multipart upload
	createMultipartUploadOutput, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		ACL:              types.ObjectCannedACLBucketOwnerFullControl,
		Bucket:           aws.String(destinationBucket),
		BucketKeyEnabled: aws.Bool(s3fs.bucketKeyEnabled),
		Key:              aws.String(destinationKey),
	})
	if err != nil {
		return err
	}

	// declared uploadId
	uploadID := createMultipartUploadOutput.UploadId

	// create parts
	parts := []struct {
		start int64
		end   int64
	}{}
	for start := int64(0); start < size; start += s3fs.partSize {
		end := start + s3fs.partSize - 1
		if end >= size {
			end = size - 1
		}
		parts = append(parts, struct {
			start int64
			end   int64
		}{
			start: start,
			end:   end,
		})
	}

	// copy parts
	etags := map[int32]*string{}
	for i, part := range parts {
		partNumber := int32(i + 1)
		uploadPartOutput, uploadPartCopyError := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:          aws.String(destinationBucket),
			CopySource:      aws.String(fmt.Sprintf("%s/%s", sourceBucket, sourceKey)),
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", part.start, part.end)),
			Key:             aws.String(destinationKey),
			PartNumber:      aws.Int32(partNumber),
			UploadId:        uploadID,
		})
		if uploadPartCopyError != nil {
			return uploadPartCopyError
		}
		etags[partNumber] = uploadPartOutput.CopyPartResult.ETag
	}

	// build list of completed parts
	completedParts := []types.CompletedPart{}
	for i := int32(1); i <= int32(len(parts)); i++ {
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       etags[i],
			PartNumber: aws.Int32(i),
		})
	}

	// complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(destinationBucket),
		Key:      aws.String(destinationKey),
		UploadId: uploadID,
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

func (s3fs *S3FileSystem) Copy(ctx context.Context, input *fs.CopyInput) error {
	if input.Logger != nil {
		_ = input.Logger.Log("Copying file", map[string]interface{}{
			"src": input.SourceName,
			"dst": input.DestinationName,
		})
	}

	if input.CheckParents {
		parent := filepath.Dir(input.DestinationName)
		if _, statError := s3fs.Stat(ctx, parent); statError != nil {
			if s3fs.IsNotExist(statError) {
				if !input.MakeParents {
					return fmt.Errorf(
						"parent directory for destination %q does not exist and parents parameter is false",
						input.DestinationName,
					)
				}
				mkdirAllError := s3fs.MkdirAll(ctx, parent, 0755)
				if mkdirAllError != nil {
					return fmt.Errorf("error creating parent directories for %q", input.DestinationName)
				}
			} else {
				return fmt.Errorf("error stating destination parent %q: %w", parent, statError)
			}
		}
	}

	// stat source file
	sourceFileSize := int64(0)
	if input.SourceFileInfo != nil {
		// input provides file info for source
		sourceFileSize = input.SourceFileInfo.Size()
	} else {
		// request file info for soruce from file system
		fi, err := s3fs.Stat(ctx, input.SourceName)
		if err != nil {
			return fmt.Errorf("error stating source file at %q: %w", input.SourceName, err)
		}
		sourceFileSize = fi.Size()
	}

	if sourceFileSize < 5_000_000_000 {
		// if soure file is less than 5 GB, then you can just use the CopyObject API
		err := s3fs.CopyObject(ctx, input.SourceName, input.DestinationName)
		if err != nil {
			return err
		}
	} else {
		err := s3fs.MultipartCopyObject(ctx, input.SourceName, input.DestinationName, sourceFileSize)
		if err != nil {
			return err
		}
	}

	if input.Logger != nil {
		_ = input.Logger.Log("Done copying file", map[string]interface{}{
			"src":  input.SourceName,
			"dst":  input.DestinationName,
			"size": sourceFileSize,
		})
	}

	return nil
}

func (s3fs *S3FileSystem) Dir(name string) string {
	return path.Dir(name)
}

// GetBucketRegion returns the region for the bucket.
// If the bucket is not known, then returns the default region
func (s3fs *S3FileSystem) GetBucketRegion(bucket string) string {
	if bucketRegion, ok := s3fs.bucketRegions[bucket]; ok {
		return bucketRegion
	}
	return s3fs.defaultRegion
}

// parse returns the bucket and key for the given name
func (s3fs *S3FileSystem) parse(name string) (string, string) {
	// if not bucket is defined
	if len(s3fs.bucket) == 0 {
		if len(s3fs.prefix) != 0 {
			panic(fmt.Errorf("invalid configuration with bucket %q and prefix %q", s3fs.bucket, s3fs.prefix))
		}
		nameParts := strings.Split(strings.TrimPrefix(name, "/"), "/")
		return nameParts[0], s3fs.Join(nameParts[1:]...)
	}
	// If prefix is defined, then append the name
	if len(s3fs.prefix) > 0 {
		return s3fs.bucket, s3fs.Join(s3fs.prefix, name)
	}
	// If no prefix is defined, then return the name as a key
	return s3fs.bucket, strings.TrimPrefix(name, "/")
}

func (s3fs *S3FileSystem) key(name string) string {
	if len(s3fs.prefix) == 0 {
		if strings.HasPrefix(name, "/") {
			return name[1:]
		}
		return name
	}
	return s3fs.Join(s3fs.prefix, name)
}

func (s3fs *S3FileSystem) HeadObject(ctx context.Context, name string) (*S3FileInfo, error) {
	bucket, key := s3fs.parse(name)
	headObjectOutput, err := s3fs.clients[s3fs.GetBucketRegion(bucket)].HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	fi := NewS3FileInfo(
		name,
		aws.ToTime(headObjectOutput.LastModified),
		aws.ToInt64(headObjectOutput.ContentLength) == int64(0),
		aws.ToInt64(headObjectOutput.ContentLength),
	)
	return fi, nil
}

func (s3fs *S3FileSystem) IsNotExist(err error) bool {
	var responseError *http.ResponseError
	if errors.As(err, &responseError) {
		if responseError.HTTPStatusCode() == 404 {
			return true
		}
	}
	return false
}

func (s3fs *S3FileSystem) Join(name ...string) string {
	return path.Join(name...)
}

func (s3fs *S3FileSystem) MagicNumber(ctx context.Context, name string) ([]byte, error) {
	data := []byte{0, 0}
	_, err := s3fs.ReadFile(ctx, name, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s3fs *S3FileSystem) MagicNumbers(ctx context.Context, names []string, threads int) ([][]byte, error) {
	magicNumbers := make([][]byte, len(names))
	for i, name := range names {
		if len(name) > 0 {
			magicNumber, err := s3fs.MagicNumber(ctx, name)
			if err != nil {
				return magicNumbers, fmt.Errorf("error retrieving magic number for file at index %d: %w", i, err)
			}
			magicNumbers[i] = magicNumber
		}
	}
	return magicNumbers, nil
}

func (s3fs *S3FileSystem) MkdirAll(ctx context.Context, name string, mode os.FileMode) error {
	bucket, key := s3fs.parse(name)
	if len(key) == 0 {
		_, err := s3fs.clients[s3fs.defaultRegion].CreateBucket(ctx, &s3.CreateBucketInput{
			ACL:    types.BucketCannedACLPrivate,
			Bucket: aws.String(bucket),
			CreateBucketConfiguration: &types.CreateBucketConfiguration{
				LocationConstraint: types.BucketLocationConstraint(s3fs.defaultRegion),
			},
			ObjectOwnership: types.ObjectOwnershipBucketOwnerEnforced,
		})
		if err != nil {
			return err
		}
		return nil
	}
	_, err := s3fs.clients[s3fs.GetBucketRegion(bucket)].PutObject(ctx, &s3.PutObjectInput{
		ACL:              types.ObjectCannedACLBucketOwnerFullControl,
		Body:             bytes.NewReader([]byte{}),
		Bucket:           aws.String(bucket),
		BucketKeyEnabled: aws.Bool(s3fs.bucketKeyEnabled),
		ContentLength:    aws.Int64(0),
		Key:              aws.String(key + "/"),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s3fs *S3FileSystem) MustRelative(ctx context.Context, base string, target string) string {
	relpath, err := s3fs.Relative(ctx, base, target)
	if err != nil {
		panic(err)
	}
	return relpath
}

func (s3fs *S3FileSystem) ReadDir(ctx context.Context, name string, recursive bool) ([]fs.DirectoryEntryInterface, error) {
	bucket := ""
	delimiter := aws.String("/")
	prefix := ""
	if len(s3fs.bucket) == 0 {
		if len(name) > 0 {
			nameParts := strings.Split(strings.TrimPrefix(name, "/"), "/")
			bucket = nameParts[0]
			if len(nameParts) > 1 {
				if k := s3fs.Join(nameParts[1:]...); k != "/" {
					prefix = k + "/"
				}
			}
		}
	} else {
		bucket = s3fs.bucket
		if name != "/" || len(s3fs.prefix) > 0 {
			prefix = s3fs.key(name) + "/"
		}
	}
	//
	directoryEntries := []fs.DirectoryEntryInterface{}
	// If listing s3 buckets in account
	if len(bucket) == 0 {
		listBucketsOutput, listBucketsError := s3fs.clients[s3fs.defaultRegion].ListBuckets(ctx, &s3.ListBucketsInput{})
		if listBucketsError != nil {
			return nil, fmt.Errorf("error listing buckets in account: %w", listBucketsError)
		}
		for _, b := range listBucketsOutput.Buckets {
			directoryEntries = append(directoryEntries, &S3DirectoryEntry{
				name:    aws.ToString(b.Name),
				dir:     true,
				modTime: aws.ToTime(b.CreationDate),
				size:    0,
			})
			if len(directoryEntries) == s3fs.maxEntries {
				break
			}
		}
		return directoryEntries, nil
	}
	// truncated, continuationToken, and startAfter are used to iterate through the bucket
	//truncated := true
	var marker *string
	// if truncated continue iterating through the bucket
	for i := 0; s3fs.maxPages == -1 || i < s3fs.maxPages; i++ {
		listObjectsInput := &s3.ListObjectsInput{
			Bucket:    aws.String(bucket),
			Delimiter: delimiter,
		}
		if s3fs.maxEntries != -1 && s3fs.maxEntries < 1000 {
			listObjectsInput.MaxKeys = aws.Int32(int32(s3fs.maxEntries))
		}
		listObjectsInput.Prefix = aws.String(prefix)
		if marker != nil {
			listObjectsInput.Marker = marker
		}
		listObjectsOutput, err := s3fs.clients[s3fs.GetBucketRegion(bucket)].ListObjects(ctx, listObjectsInput)
		if err != nil {
			return nil, err
		}
		if s3fs.maxEntries != -1 {
			// limit on number of directory entries
			for _, commonPrefix := range listObjectsOutput.CommonPrefixes {
				directoryPrefix := strings.TrimRight(aws.ToString(commonPrefix.Prefix), "/")
				directoryName := ""
				if len(s3fs.bucket) == 0 {
					// if root is s3://
					if strings.HasPrefix(name, "/") {
						directoryName = strings.TrimPrefix("/"+s3fs.Join(bucket, directoryPrefix), name+"/")
					} else {
						directoryName = strings.TrimPrefix(s3fs.Join(bucket, directoryPrefix), name+"/")
					}
				} else if len(s3fs.prefix) > 0 {
					directoryName = strings.TrimPrefix(strings.TrimPrefix(directoryPrefix, s3fs.prefix), name+"/")
				} else {
					directoryName = strings.TrimPrefix("/"+directoryPrefix, name+"/")
				}
				if directoryName == "" {
					panic("directoryName is empty, which should never happen")
				}
				directoryEntries = append(directoryEntries, &S3DirectoryEntry{
					name:    directoryName,
					dir:     true,
					modTime: s3fs.bucketCreationDates[s3fs.bucket],
					size:    0,
				})
				if len(directoryEntries) == s3fs.maxEntries {
					break
				}
			}
			if len(directoryEntries) == s3fs.maxEntries {
				break
			}
			for _, object := range listObjectsOutput.Contents {
				fileName := ""
				if len(s3fs.bucket) == 0 {
					if strings.HasSuffix(aws.ToString(object.Key), "/") {
						// if key ends in slash, then add it back since s3fs.Join strips trailing slashes
						fileName = strings.TrimPrefix("/"+s3fs.Join(bucket, aws.ToString(object.Key))+"/", name+"/")
					} else {
						fileName = strings.TrimPrefix("/"+s3fs.Join(bucket, aws.ToString(object.Key)), name+"/")
					}
				} else if len(s3fs.prefix) > 0 {
					fileName = strings.TrimPrefix(strings.TrimPrefix(aws.ToString(object.Key), s3fs.prefix), name+"/")
				} else {
					fileName = strings.TrimPrefix("/"+aws.ToString(object.Key), name+"/")
				}
				// //
				// fmt.Println("---------------------------")
				// fmt.Println("name:", name)
				// fmt.Println("s3fs.bucket:", s3fs.bucket)
				// fmt.Println("s3fs.prefix:", s3fs.prefix)
				// fmt.Println("bucket:", bucket)
				// fmt.Println("prefix:", prefix)
				// fmt.Println("objectKey:",  aws.ToString(object.Key))
				// fmt.Println("fileName:", fileName)
				// fileName is a blank string then there is a directory marker in s3,
				// and you returned itself, so you can safely skip this one.
				if fileName != "" {
					directoryEntries = append(directoryEntries, &S3DirectoryEntry{
						name:    fileName,
						dir:     (aws.ToInt64(object.Size) == 0) && strings.HasSuffix(aws.ToString(object.Key), "/"),
						modTime: aws.ToTime(object.LastModified),
						size:    aws.ToInt64(object.Size),
					})
				}
				if len(directoryEntries) == s3fs.maxEntries {
					break
				}
			}
			if len(directoryEntries) == s3fs.maxEntries {
				break
			}
		} else {
			// no limit for number of directory entries
			for _, commonPrefix := range listObjectsOutput.CommonPrefixes {
				directoryPrefix := strings.TrimRight(aws.ToString(commonPrefix.Prefix), "/")
				directoryName := ""
				if len(s3fs.bucket) == 0 {
					// if root is s3://
					if strings.HasPrefix(name, "/") {
						directoryName = strings.TrimPrefix("/"+s3fs.Join(bucket, directoryPrefix), name+"/")
					} else {
						directoryName = strings.TrimPrefix(s3fs.Join(bucket, directoryPrefix), name+"/")
					}
				} else if len(s3fs.prefix) > 0 {
					directoryName = strings.TrimPrefix(strings.TrimPrefix(directoryPrefix, s3fs.prefix), name+"/")
				} else {
					directoryName = strings.TrimPrefix("/"+directoryPrefix, name+"/")
				}
				if directoryName == "" {
					panic("directoryName is empty, which should never happen")
				}
				directoryEntries = append(directoryEntries, &S3DirectoryEntry{
					name:    directoryName,
					dir:     true,
					modTime: s3fs.bucketCreationDates[s3fs.bucket],
					size:    0,
				})
			}
			for _, object := range listObjectsOutput.Contents {
				fileName := ""
				if len(s3fs.bucket) == 0 {
					if strings.HasSuffix(aws.ToString(object.Key), "/") {
						// if key ends in slash, then add it back since s3fs.Join strips trailing slashes
						fileName = strings.TrimPrefix("/"+s3fs.Join(bucket, aws.ToString(object.Key))+"/", name+"/")
					} else {
						fileName = strings.TrimPrefix("/"+s3fs.Join(bucket, aws.ToString(object.Key)), name+"/")
					}
				} else if len(s3fs.prefix) > 0 {
					fileName = strings.TrimPrefix(strings.TrimPrefix(aws.ToString(object.Key), s3fs.prefix), name+"/")
				} else {
					fileName = strings.TrimPrefix("/"+aws.ToString(object.Key), name+"/")
				}
				// fileName is a blank string then there is a directory marker in s3,
				// and you returned itself, so you can safely skip this one.
				if fileName != "" {
					directoryEntries = append(directoryEntries, &S3DirectoryEntry{
						name:    fileName,
						dir:     (aws.ToInt64(object.Size) == 0) && strings.HasSuffix(aws.ToString(object.Key), "/"),
						modTime: aws.ToTime(object.LastModified),
						size:    aws.ToInt64(object.Size),
					})
				}
			}
		}
		if !aws.ToBool(listObjectsOutput.IsTruncated) {
			break
		}
		marker = listObjectsOutput.NextMarker
	}

	// if recursive, then read sub directories
	if recursive {
		recursiveDirectoryEntries := []fs.DirectoryEntryInterface{}
		for _, directoryEntry := range directoryEntries {
			if directoryEntry.IsDir() {
				moreDirectoryEntries, err := s3fs.ReadDir(ctx, s3fs.Join(name, directoryEntry.Name()), recursive)
				if err != nil {
					return nil, err
				}
				for _, de := range moreDirectoryEntries {
					recursiveDirectoryEntries = append(recursiveDirectoryEntries, fs.NewDirectoryEntry(
						s3fs.Join(directoryEntry.Name(), de.Name()),
						de.IsDir(),
						de.ModTime(),
						de.Size()),
					)
				}
			}
		}
		directoryEntries = append(directoryEntries, recursiveDirectoryEntries...)
	}

	return directoryEntries, nil
}

func (s3fs *S3FileSystem) ReadFile(ctx context.Context, name string, data []byte) (int, error) {
	file, err := s3fs.Open(ctx, name)
	if err != nil {
		return 0, err
	}
	n, err := file.Read(data)
	if err != nil {
		return n, err
	}
	err = file.Close()
	if err != nil {
		return n, err
	}
	return n, nil
}

func (s3fs *S3FileSystem) Relative(ctx context.Context, base string, target string) (string, error) {
	// if target is blank, then cannot make relative path
	if target == "" {
		return "", errors.New("Rel: can't make " + target + " relative to " + base)
	}
	// if paths are equal
	if path.Clean(base) == path.Clean(target) {
		return ".", nil
	}
	// if base is root
	if base == "/" {
		if target[0] == '/' {
			return target, nil
		}
	}
	prefix := base
	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	// if base is parent of target
	if strings.HasPrefix(target, prefix) {
		return path.Clean(strings.TrimPrefix(target, prefix)), nil
	}

	// if base and target are both absolute
	if base[0] == '/' && target[0] == '/' {
		return s3fs.Join(strings.Repeat("../", len(strings.Split(strings.TrimSuffix(strings.TrimPrefix(base, "/"), "/"), "/"))), target), nil
	}
	return "", errors.New("Rel: can't make " + target + " relative to " + base)
}

func (s3fs *S3FileSystem) RemoveFile(ctx context.Context, name string) error {
	err := s3fs.DeleteObject(ctx, name)
	if err != nil {
		return fmt.Errorf("error removing file with name %q: %w", name, err)
	}
	return nil
}

func (s3fs *S3FileSystem) RemoveFiles(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	err := s3fs.DeleteObjects(ctx, names)
	if err != nil {
		return fmt.Errorf("error removing %d files: %w", len(names), err)
	}
	return nil
}

func (s3fs *S3FileSystem) RemoveDirectory(ctx context.Context, name string, recursive bool) error {

	children, err := s3fs.ReadDir(ctx, name, false)
	if err != nil {
		if os.IsExist(err) && recursive {
			return nil
		}
		return fmt.Errorf("error reading directory %q: %w", name, err)
	}

	if len(children) > 0 {
		if !recursive {
			return fmt.Errorf("error deleting directory: %q is not empty", name)
		}
		filesToBeDeleted := []string{}
		directoriesToBeDeleted := []string{}
		for _, directoryEntry := range children {
			if directoryEntry.IsDir() {
				directoriesToBeDeleted = append(directoriesToBeDeleted, filepath.Join(name, directoryEntry.Name()))
			} else {
				filesToBeDeleted = append(filesToBeDeleted, filepath.Join(name, directoryEntry.Name()))
			}
		}
		fmt.Fprintln(os.Stderr, "Deleting directories:", directoriesToBeDeleted)
		// removing directories
		for _, directoryName := range directoriesToBeDeleted {
			err := s3fs.RemoveDirectory(ctx, directoryName, true)
			if err != nil {
				return err
			}
		}
		fmt.Fprintln(os.Stderr, "Deleting files:", filesToBeDeleted)
		// removing files
		err := s3fs.DeleteObjects(ctx, filesToBeDeleted)
		if err != nil {
			return fmt.Errorf("error removing %d files: %w", len(filesToBeDeleted), err)
		}
		return nil
	}

	return nil
}

func (s3fs *S3FileSystem) RemoveDirectories(ctx context.Context, names []string, recursive bool) error {
	if len(names) == 0 {
		return nil
	}
	for i, name := range names {
		err := s3fs.RemoveDirectory(ctx, name, recursive)
		if err != nil {
			return fmt.Errorf("error removing directory %q at index %d: %w", name, i, err)
		}
	}
	return nil
}

func (s3fs *S3FileSystem) Root() string {
	if len(s3fs.bucket) == 0 {
		return "s3://"
	}
	return fmt.Sprintf("s3://%s%s", s3fs.bucket, s3fs.prefix)
}

func (s3fs *S3FileSystem) Size(ctx context.Context, name string) (int64, error) {
	bucket, key := s3fs.parse(name)
	headObjectOutput, err := s3fs.clients[s3fs.GetBucketRegion(bucket)].HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return int64(0), err
	}
	return aws.ToInt64(headObjectOutput.ContentLength), nil
}

func (s3fs *S3FileSystem) Stat(ctx context.Context, name string) (fs.FileInfo, error) {
	if len(s3fs.bucket) == 0 {
		if len(s3fs.prefix) != 0 {
			return nil, fmt.Errorf("invalid configuration with bucket %q and prefix %q", s3fs.bucket, s3fs.prefix)
		}
		if name == "/" {
			fi := NewS3FileInfo(
				name,
				s3fs.earliestCreationDate,
				true,
				int64(0),
			)
			return fi, nil
		}
		nameParts := strings.Split(strings.TrimPrefix(name, "/"), "/")
		if len(nameParts) == 1 {
			bucket := nameParts[0]
			_, err := s3fs.clients[s3fs.GetBucketRegion(bucket)].HeadBucket(ctx, &s3.HeadBucketInput{
				Bucket: aws.String(bucket),
			})
			if err != nil {
				return nil, err
			}
			fi := NewS3FileInfo(
				name,
				s3fs.bucketCreationDates[nameParts[0]],
				true,
				int64(0),
			)
			return fi, nil
		}
		directoryEntries, readDirError := s3fs.ReadDir(ctx, name, false)
		if readDirError != nil {
			return nil, fmt.Errorf("error reading directory %q: %w", name, readDirError)
		}
		if len(directoryEntries) > 0 {
			fi := NewS3FileInfo(
				name,
				s3fs.bucketCreationDates[nameParts[0]], // set creation date to the creation date of the bucket
				true,
				int64(0),
			)
			return fi, nil
		}
		// no directory entires, so this must be a file
		fi, err := s3fs.HeadObject(ctx, name)
		if err != nil {
			return nil, err
		}
		return fi, nil
	} else {
		// if bucket is defined
		if name == "/" && len(s3fs.prefix) == 0 {
			_, err := s3fs.clients[s3fs.GetBucketRegion(s3fs.bucket)].HeadBucket(ctx, &s3.HeadBucketInput{
				Bucket: aws.String(s3fs.bucket),
			})
			if err != nil {
				return nil, err
			}
			fi := NewS3FileInfo(
				name,
				s3fs.bucketCreationDates[s3fs.bucket],
				true,
				int64(0),
			)
			return fi, nil
		}
	}

	directoryEntries, readDirError := s3fs.ReadDir(ctx, name, false)
	if readDirError != nil {
		return nil, fmt.Errorf("error reading directory %q: %w", name, readDirError)
	}
	if len(directoryEntries) > 0 {
		fi := NewS3FileInfo(
			name,
			s3fs.bucketCreationDates[s3fs.bucket],
			true,
			int64(0),
		)
		return fi, nil
	}

	fi, err := s3fs.HeadObject(ctx, name)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

func (s3fs *S3FileSystem) Open(ctx context.Context, name string) (fs.Object, error) {
	size, sizeError := s3fs.Size(ctx, name)
	if sizeError != nil {
		return nil, sizeError
	}
	bucket, key := s3fs.parse(name)
	client := s3fs.clients[s3fs.GetBucketRegion(bucket)]
	readSeeker := NewReadSeeker(
		0,
		size,
		func(offset int64, p []byte) (int, error) {
			getObjectOutput, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, int(offset)+len(p)-1)),
			})
			if err != nil {
				return 0, err
			}
			body, err := io.ReadAll(getObjectOutput.Body)
			if err != nil {
				return 0, err
			}
			copy(p, body)
			return len(body), nil
		},
	)
	downloader := Downloader(func(ctx context.Context, w io.WriterAt) (int64, error) {
		// create wait group
		wg := errgroup.Group{}
		// set limit
		wg.SetLimit(runtime.NumCPU())
		// declare parts array
		parts := []struct {
			start int64
			end   int64
		}{}
		// Create parts
		for offset := int64(0); offset < size; offset += s3fs.partSize {
			start := offset
			end := start + s3fs.partSize - 1
			if end >= size {
				end = size - 1
			}
			parts = append(parts, struct {
				start int64
				end   int64
			}{start: start, end: end})
		}
		writtenByPart := make([]int64, len(parts))
		// iterate through parts
		for i, part := range parts {
			i := i
			start := part.start
			end := part.end
			wg.Go(func() error {
				getObjectOutput, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
				})
				if err != nil {
					return fmt.Errorf(
						"error reading object %q at range %d-%d: %w",
						fmt.Sprintf("s3://%s/%s", bucket, key),
						start,
						end,
						err)
				}
				body, err := io.ReadAll(getObjectOutput.Body)
				if err != nil {
					return fmt.Errorf(
						"error reading body for object %q at range %d-%d: %w",
						fmt.Sprintf("s3://%s/%s", bucket, key),
						start,
						end,
						err)
				}
				n, err := w.WriteAt(body, start)
				if err != nil {
					return fmt.Errorf(
						"error writing object %q at range %d-%d: %w",
						fmt.Sprintf("s3://%s/%s", bucket, key),
						start,
						end,
						err)
				}
				writtenByPart[i] = int64(n)
				return nil
			})
		}

		// Wait until all parts have been downloaded
		err := wg.Wait()

		// sum number of bytes written
		sum := int64(0)
		for _, n := range writtenByPart {
			sum += n
		}

		// return sum and error if any
		return sum, err
	})
	return NewS3File(name, readSeeker, downloader, nil), nil
}

func (s3fs *S3FileSystem) OpenObject(ctx context.Context, name string, flag int, perm os.FileMode) (fs.Object, error) {
	doesNotExist := false
	size, sizeError := s3fs.Size(ctx, name)
	if sizeError != nil {
		if s3fs.IsNotExist(sizeError) {
			doesNotExist = true
		} else {
			return nil, sizeError
		}
	}

	bucket, key := s3fs.parse(name)
	client := s3fs.clients[s3fs.GetBucketRegion(bucket)]

	var readSeeker *ReadSeeker
	var downloader Downloader
	uploader := NewUploader(ctx, &UploaderInput{
		ACL:              types.ObjectCannedACLBucketOwnerFullControl,
		Client:           client,
		Bucket:           bucket,
		BucketKeyEnabled: s3fs.bucketKeyEnabled,
		Key:              key,
		PartSize:         int(s3fs.partSize),
	})
	if !doesNotExist {
		readSeeker = NewReadSeeker(
			0,
			size,
			func(offset int64, p []byte) (int, error) {
				fmt.Println("client.GetObject:", bucket, key, offset)
				getObjectOutput, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, int(offset)+len(p)-1)),
				})
				if err != nil {
					return 0, err
				}
				body, err := io.ReadAll(getObjectOutput.Body)
				if err != nil {
					return 0, err
				}
				copy(p, body)
				return len(body), nil
			},
		)
		downloader = Downloader(func(ctx context.Context, w io.WriterAt) (int64, error) {
			// create wait group
			wg := errgroup.Group{}
			// set limit
			wg.SetLimit(runtime.NumCPU())
			// declare parts array
			parts := []struct {
				start int64
				end   int64
			}{}
			// Create parts
			for offset := int64(0); offset < size; offset += s3fs.partSize {
				start := offset
				end := start + s3fs.partSize - 1
				if end >= size {
					end = size - 1
				}
				parts = append(parts, struct {
					start int64
					end   int64
				}{start: start, end: end})
			}
			writtenByPart := make([]int64, len(parts))
			// iterate through parts
			for i, part := range parts {
				i := i
				start := part.start
				end := part.end
				wg.Go(func() error {
					getObjectOutput, err := client.GetObject(ctx, &s3.GetObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(key),
						Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
					})
					if err != nil {
						return fmt.Errorf(
							"error reading object %q at range %d-%d: %w",
							fmt.Sprintf("s3://%s/%s", bucket, key),
							start,
							end,
							err)
					}
					body, err := io.ReadAll(getObjectOutput.Body)
					if err != nil {
						return fmt.Errorf(
							"error reading body for object %q at range %d-%d: %w",
							fmt.Sprintf("s3://%s/%s", bucket, key),
							start,
							end,
							err)
					}
					n, err := w.WriteAt(body, start)
					if err != nil {
						return fmt.Errorf(
							"error writing object %q at range %d-%d: %w",
							fmt.Sprintf("s3://%s/%s", bucket, key),
							start,
							end,
							err)
					}
					writtenByPart[i] = int64(n)
					return nil
				})
			}

			// Wait until all parts have been downloaded
			err := wg.Wait()

			// sum number of bytes written
			sum := int64(0)
			for _, n := range writtenByPart {
				sum += n
			}

			// return sum and error if any
			return sum, err
		})
	}

	return NewS3File(name, readSeeker, downloader, uploader), nil
}

func (s3fs *S3FileSystem) SyncDirectory(ctx context.Context, input *fs.SyncDirectoryInput) (int, error) {

	// limit is zero
	if input.Limit == 0 {
		return 0, nil
	}

	skip := false
	if len(input.Exclude) > 0 {
		for _, exclude := range input.Exclude {
			if strings.HasPrefix(exclude, "*") {
				if strings.HasSuffix(exclude, "*") {
					if strings.Contains(input.SourceDirectory, exclude[1:len(exclude)-1]) {
						skip = true
						break
					}
				} else {
					if strings.HasSuffix(input.SourceDirectory, exclude[1:]) {
						skip = true
						break
					}
				}
			} else {
				if strings.HasSuffix(exclude, "*") {
					if strings.HasPrefix(input.SourceDirectory, exclude[0:len(exclude)-1]) {
						skip = true
						break
					}
				} else {
					if input.SourceDirectory == exclude {
						skip = true
						break
					}
				}
			}
		}
	}

	if skip {
		if input.Logger != nil {
			_ = input.Logger.Log("Skipping directory", map[string]interface{}{
				"src": input.SourceDirectory,
			})
		}
		return 0, nil
	}

	// list source directory
	sourceDirectoryEntriesByName := map[string]fs.DirectoryEntryInterface{}
	sourceDirectoryEntries, err := s3fs.ReadDir(ctx, input.SourceDirectory, false)
	if err != nil {
		return 0, fmt.Errorf("error reading source directory %q: %w", input.SourceDirectory, err)
	} else {
		for _, sourceDirectoryEntry := range sourceDirectoryEntries {
			if input.SourceDirectory == sourceDirectoryEntry.Name() {
				return 0, errors.New("the directory entry cannot have the same path as its parent:" + sourceDirectoryEntry.Name())
			}
			sourceName := filepath.Join(input.SourceDirectory, sourceDirectoryEntry.Name())
			sourceDirectoryEntriesByName[sourceName] = sourceDirectoryEntry
		}
	}

	// list destination directory to cache file info
	destinationDirectoryEntriesByName := map[string]fs.DirectoryEntryInterface{}
	destinationDirectoryEntries, err := s3fs.ReadDir(ctx, input.DestinationDirectory, false)
	if err != nil {
		if !s3fs.IsNotExist(err) {
			return 0, fmt.Errorf("error reading destination directory %q: %w", input.SourceDirectory, err)
		}
	} else {
		for _, destinationDirectoryEntry := range destinationDirectoryEntries {
			destinationName := filepath.Join(input.DestinationDirectory, destinationDirectoryEntry.Name())
			destinationDirectoryEntriesByName[destinationName] = destinationDirectoryEntry
		}
	}

	if input.Logger != nil {
		_ = input.Logger.Log("Synchronizing Directory", map[string]interface{}{
			"delete":  input.Delete,
			"dst":     input.DestinationDirectory,
			"exclude": input.Exclude,
			"files":   len(sourceDirectoryEntries),
			"src":     input.SourceDirectory,
			"threads": input.MaxThreads,
		})
	}

	// create directory if not exist
	if _, statError := s3fs.Stat(ctx, input.DestinationDirectory); statError != nil {
		if s3fs.IsNotExist(statError) {
			mkdirAllError := s3fs.MkdirAll(ctx, input.DestinationDirectory, 0755)
			if mkdirAllError != nil {
				return 0, fmt.Errorf("error creating destination directory for %q: %w", input.DestinationDirectory, mkdirAllError)
			}
		} else {
			return 0, fmt.Errorf("error stating destination directory %q: %w", input.DestinationDirectory, statError)
		}
	}

	// delete files at destination that do not exist at source
	if input.Delete {
		filesToBeDeleted := []string{}
		directoriesToBeDeleted := []string{}
		for destinationDirectoryName, destinationDirectoryEntry := range destinationDirectoryEntriesByName {
			destinationSuffix := destinationDirectoryName[len(input.DestinationDirectory):]
			sourcePath := s3fs.Join(input.SourceDirectory, destinationSuffix)
			if _, ok := sourceDirectoryEntriesByName[sourcePath]; !ok {
				if destinationDirectoryEntry.IsDir() {
					directoriesToBeDeleted = append(directoriesToBeDeleted, destinationDirectoryName)
				} else {
					filesToBeDeleted = append(filesToBeDeleted, destinationDirectoryName)
				}
			}
		}
		_ = input.Logger.Log("Deleting directories", map[string]interface{}{
			"directories": directoriesToBeDeleted,
		})
		err := s3fs.RemoveDirectories(ctx, directoriesToBeDeleted, true)
		if err != nil {
			return 0, fmt.Errorf("error removing %d directories at destination that do not exist at source: %w", len(directoriesToBeDeleted), err)
		}
		_ = input.Logger.Log("Deleting files", map[string]interface{}{
			"files": filesToBeDeleted,
		})
		err = s3fs.RemoveFiles(ctx, filesToBeDeleted)
		if err != nil {
			return 0, fmt.Errorf("error removing %d files at destination that do not exist at source: %w", len(filesToBeDeleted), err)
		}
	}

	// wait group
	var wg errgroup.Group

	// If input.MaxThreads is greater than zero, than set limit.
	if input.MaxThreads > 0 {
		wg.SetLimit(input.MaxThreads)
	}

	// declare count
	count := 0

	for _, sourceDirectoryEntry := range sourceDirectoryEntries {
		sourceDirectoryEntry := sourceDirectoryEntry
		sourceName := filepath.Join(input.SourceDirectory, sourceDirectoryEntry.Name())
		destinationName := filepath.Join(input.DestinationDirectory, sourceDirectoryEntry.Name())
		if sourceDirectoryEntry.IsDir() {
			// synchronize directory and wait until all files are finished copying
			directoryLimit := -1
			if input.Limit != -1 {
				directoryLimit = input.Limit - count
			}
			c, err := s3fs.SyncDirectory(ctx, &fs.SyncDirectoryInput{
				CheckTimestamps:      input.CheckTimestamps,
				Delete:               input.Delete,
				Exclude:              input.Exclude,
				SourceDirectory:      sourceName,
				DestinationDirectory: destinationName,
				Limit:                directoryLimit,
				Logger:               input.Logger,
				MaxThreads:           input.MaxThreads,
				TimestampPrecision:   input.TimestampPrecision,
			})
			if err != nil {
				return 0, err
			}
			count += c
		} else {
			// check if file should be skipped
			skip := false
			if len(input.Exclude) > 0 {
				for _, exclude := range input.Exclude {
					if strings.HasPrefix(exclude, "*") {
						if strings.HasSuffix(exclude, "*") {
							if strings.Contains(sourceName, exclude[1:len(exclude)-1]) {
								skip = true
								break
							}
						} else {
							if strings.HasSuffix(sourceName, exclude[1:]) {
								skip = true
								break
							}
						}
					} else {
						if strings.HasSuffix(exclude, "*") {
							if strings.HasPrefix(sourceName, exclude[0:len(exclude)-1]) {
								skip = true
								break
							}
						} else {
							if sourceName == exclude {
								skip = true
								break
							}
						}
					}
				}
			}
			if skip {
				if input.Logger != nil {
					_ = input.Logger.Log("Skipping file", map[string]interface{}{
						"src": sourceName,
					})
				}
			} else {
				count += 1
				wg.Go(func() error {
					copyFile := false
					// declare destination file info
					var destinationFileInfo fs.FileInfo
					if dfi, ok := destinationDirectoryEntriesByName[destinationName]; ok {
						// use cached destination file info
						destinationFileInfo = dfi
						// compare sizes
						if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
							copyFile = true
						}
						// compare timestamps
						if input.CheckTimestamps {
							if !fs.EqualTimestamp(sourceDirectoryEntry.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
								copyFile = true
							}
						}
					} else {
						// destination not cached
						dfi, err := s3fs.Stat(ctx, destinationName)
						if err != nil {
							if s3fs.IsNotExist(err) {
								copyFile = true
							} else {
								return fmt.Errorf("error stating destination %q: %w", destinationName, err)
							}
						} else {
							// set destination file info
							destinationFileInfo = dfi
							// compare sizes
							if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
								copyFile = true
							}
							// compare timestamps
							if input.CheckTimestamps {
								if !fs.EqualTimestamp(sourceDirectoryEntry.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
									copyFile = true
								}
							}
						}
					}
					if copyFile {
						err := s3fs.Copy(context.Background(), &fs.CopyInput{
							CheckParents:        false, // parent directory already created
							SourceName:          sourceName,
							SourceFileInfo:      sourceDirectoryEntry,
							DestinationName:     destinationName,
							DestinationFileInfo: destinationFileInfo,
							Logger:              input.Logger,
							MakeParents:         false, // parent directory already created
						})
						if err != nil {
							return fmt.Errorf("error copying %q to %q: %w", sourceName, destinationName, err)
						}
					}
					return nil
				})
			}
		}
		// break if count is greater than or at the limit
		if input.Limit != -1 && count >= input.Limit {
			break
		}
	}

	// wait for all files in directory to copy before returning
	if err := wg.Wait(); err != nil {
		return 0, fmt.Errorf("error synchronizing directory %q to %q: %w", input.SourceDirectory, input.DestinationDirectory, err)
	}

	return count, nil
}

func (s3fs *S3FileSystem) Sync(ctx context.Context, input *fs.SyncInput) (int, error) {

	if !strings.HasPrefix(input.Source, "/") {
		return 0, fmt.Errorf("source %q does not start with /", input.Source)
	}

	if !strings.HasPrefix(input.Destination, "/") {
		return 0, fmt.Errorf("destination %q does not start with /", input.Destination)
	}

	if input.Logger != nil {
		_ = input.Logger.Log("Synchronizing", map[string]interface{}{
			"src":     input.Source,
			"dst":     input.Destination,
			"threads": input.MaxThreads,
			"exclude": input.Exclude,
		})
	}

	skip := false
	if len(input.Exclude) > 0 {
		for _, exclude := range input.Exclude {
			if strings.HasPrefix(exclude, "*") {
				if strings.HasSuffix(exclude, "*") {
					if strings.Contains(input.Source, exclude[1:len(exclude)-1]) {
						skip = true
						break
					}
				} else {
					if strings.HasSuffix(input.Source, exclude[1:]) {
						skip = true
						break
					}
				}
			} else {
				if strings.HasSuffix(exclude, "*") {
					if strings.HasPrefix(input.Source, exclude[0:len(exclude)-1]) {
						skip = true
						break
					}
				} else {
					if input.Source == exclude {
						skip = true
						break
					}
				}
			}
		}
	}

	if skip {
		if input.Logger != nil {
			_ = input.Logger.Log("Skipping source", map[string]interface{}{
				"src": input.Source,
			})
		}
		return 0, nil
	}

	sourceFileInfo, err := s3fs.Stat(ctx, input.Source)
	if err != nil {
		if s3fs.IsNotExist(err) {
			return 0, fmt.Errorf("source does not exist %q: %w", input.Source, err)
		}
		return 0, fmt.Errorf("error stating source %q: %w", input.Source, err)
	}

	if len(s3fs.bucket) == 0 && !input.Parents {
		destinationBucket := strings.Split(strings.TrimPrefix(input.Destination, "/"), "/")[0]
		_, statError := s3fs.Stat(ctx, destinationBucket)
		if statError != nil {
			if s3fs.IsNotExist(statError) {
				return 0, fmt.Errorf("bucket for destination does not exist %q", destinationBucket)
			}
			return 0, fmt.Errorf("error stating destination bucket %q", destinationBucket)
		}
	}

	// if source is a directory
	if sourceFileInfo.IsDir() {
		if _, statError := s3fs.Stat(ctx, input.Destination); statError != nil {
			if s3fs.IsNotExist(statError) {
				if !input.Parents {
					return 0, fmt.Errorf("destination directory does not exist and parents is false: %q", input.Destination)
				}
			}
		}
		count, syncDirectoryError := s3fs.SyncDirectory(ctx, &fs.SyncDirectoryInput{
			CheckTimestamps:      input.CheckTimestamps,
			Delete:               input.Delete,
			DestinationDirectory: input.Destination,
			Exclude:              input.Exclude,
			Limit:                input.Limit,
			Logger:               input.Logger,
			MaxThreads:           input.MaxThreads,
			SourceDirectory:      input.Source,
			TimestampPrecision:   input.TimestampPrecision,
		})
		if syncDirectoryError != nil {
			return 0, fmt.Errorf(
				"error syncing source directory %q to destination directory %q: %w",
				input.Source,
				input.Destination,
				syncDirectoryError)
		}
		return count, nil
	}

	// if source is a file
	copyFile := false

	destinationFileInfo, err := s3fs.Stat(ctx, input.Destination)
	if err != nil {
		if s3fs.IsNotExist(err) {
			copyFile = true
		} else {
			return 0, fmt.Errorf("error stating destination %q: %w", input.Destination, err)
		}
	} else {
		if sourceFileInfo.Size() != destinationFileInfo.Size() {
			copyFile = true
		}
		if input.CheckTimestamps {
			if !fs.EqualTimestamp(sourceFileInfo.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
				copyFile = true
			}
		}
	}

	if copyFile {
		err = s3fs.Copy(ctx, &fs.CopyInput{
			CheckParents:    true, // check if parent for destination exists
			SourceName:      input.Source,
			DestinationName: input.Destination,
			Logger:          input.Logger,
			MakeParents:     input.Parents, // create destination parents if not exist
		})
		if err != nil {
			return 0, fmt.Errorf("error copying %q to %q: %w", input.Source, input.Destination, err)
		}
		return 1, nil
	} else {
		if input.Logger != nil {
			_ = input.Logger.Log("Skipping file", map[string]interface{}{
				"src": input.Source,
			})
		}
	}

	return 0, nil
}

func NewS3FileSystem(
	defaultRegion string,
	bucket string,
	prefix string,
	clients map[string]*s3.Client,
	bucketRegions map[string]string,
	bucketCreationDates map[string]time.Time,
	maxEntries int,
	maxPages int,
	bucketKeyEnabled bool,
	partSize int) *S3FileSystem {

	// calculate earliest creation date
	earliestCreationDate := time.Time{}
	for _, t := range bucketCreationDates {
		if t.Before(earliestCreationDate) {
			earliestCreationDate = t
		}
	}
	// return new file system
	return &S3FileSystem{
		bucket:               bucket,
		prefix:               prefix,
		clients:              clients,
		defaultRegion:        defaultRegion,
		bucketRegions:        bucketRegions,
		bucketCreationDates:  bucketCreationDates,
		earliestCreationDate: earliestCreationDate,
		maxEntries:           maxEntries,
		maxPages:             maxPages,
		bucketKeyEnabled:     bucketKeyEnabled,
		partSize:             int64(partSize),
	}
}
