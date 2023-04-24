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

	"github.com/deptofdefense/gosync/pkg/fs"
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

func (s3fs *S3FileSystem) Copy(ctx context.Context, input *fs.CopyInput) error {
	if input.Logger != nil {
		input.Logger.Log("Copying file", map[string]interface{}{
			"src": input.SourceName,
			"dst": input.DestinationName,
		})
	}

	sourceBucket, sourceKey := s3fs.parse(input.SourceName)

	destinationBucket, destinationKey := s3fs.parse(input.DestinationName)

	_, err := s3fs.clients[s3fs.GetBucketRegion(destinationBucket)].CopyObject(ctx, &s3.CopyObjectInput{
		ACL:              types.ObjectCannedACLBucketOwnerFullControl,
		Bucket:           aws.String(destinationBucket),
		BucketKeyEnabled: s3fs.bucketKeyEnabled,
		Key:              aws.String(destinationKey),
		CopySource:       aws.String(fmt.Sprintf("%s/%s", sourceBucket, sourceKey)),
	})
	if err != nil {
		return err
	}

	if input.Logger != nil {
		input.Logger.Log("Done copying file", map[string]interface{}{
			"src": input.SourceName,
			"dst": input.DestinationName,
		})
	}

	return nil
}

func (s3fs *S3FileSystem) Dir(name string) string {
	return path.Dir(name)
}

// GetRegion returns the region for the bucket.
// If the bucket is not known, then returns the default region
func (s3fs *S3FileSystem) GetBucketRegion(bucket string) string {
	if bucketRegion, ok := s3fs.bucketRegions[bucket]; ok {
		return bucketRegion
	}
	return s3fs.defaultRegion
}

// parse returns the bucket and key for the given name
func (fs *S3FileSystem) parse(name string) (string, string) {
	// if not bucket is defined
	if len(fs.bucket) == 0 {
		if len(fs.prefix) != 0 {
			panic(fmt.Errorf("invalid configuration with bucket %q and prefix %q", fs.bucket, fs.prefix))
		}
		nameParts := strings.Split(strings.TrimPrefix(name, "/"), "/")
		return nameParts[0], fs.Join(nameParts[1:]...)
	}
	// If prefix is defined, then append the name
	if len(fs.prefix) > 0 {
		return fs.bucket, fs.Join(fs.prefix, name)
	}
	// If no prefix is defined, then return the name as a key
	return fs.bucket, strings.TrimPrefix(name, "/")
}

func (fs *S3FileSystem) key(name string) string {
	if len(fs.prefix) == 0 {
		if strings.HasPrefix(name, "/") {
			return name[1:]
		}
		return name
	}
	return fs.Join(fs.prefix, name)
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
		headObjectOutput.ContentLength == int64(0),
		headObjectOutput.ContentLength,
	)
	return fi, nil
}

func (fs *S3FileSystem) IsNotExist(err error) bool {
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

func (s3fs *S3FileSystem) MkdirAll(ctx context.Context, name string, mode os.FileMode) error {
	bucket, key := s3fs.parse(name)
	_, err := s3fs.clients[s3fs.GetBucketRegion(bucket)].PutObject(ctx, &s3.PutObjectInput{
		ACL:              types.ObjectCannedACLBucketOwnerFullControl,
		Body:             bytes.NewReader([]byte{}),
		Bucket:           aws.String(bucket),
		BucketKeyEnabled: s3fs.bucketKeyEnabled,
		ContentLength:    int64(0),
		Key:              aws.String(key + "/"),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s3fs *S3FileSystem) ReadDir(ctx context.Context, name string) ([]fs.DirectoryEntry, error) {
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
	directoryEntries := []fs.DirectoryEntry{}
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
			listObjectsInput.MaxKeys = int32(s3fs.maxEntries)
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
					fileName = strings.TrimPrefix("/"+s3fs.Join(bucket, aws.ToString(object.Key)), name+"/")
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
						dir:     (object.Size == 0),
						modTime: aws.ToTime(object.LastModified),
						size:    object.Size,
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
					fileName = strings.TrimPrefix("/"+s3fs.Join(bucket, aws.ToString(object.Key)), name+"/")
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
						dir:     (object.Size == 0),
						modTime: aws.ToTime(object.LastModified),
						size:    object.Size,
					})
				}
			}
		}
		if !listObjectsOutput.IsTruncated {
			break
		}
		marker = listObjectsOutput.NextMarker
	}

	return directoryEntries, nil
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
	return headObjectOutput.ContentLength, nil
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
		directoryEntries, readDirError := s3fs.ReadDir(ctx, name)
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

	directoryEntries, readDirError := s3fs.ReadDir(ctx, name)
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

func (s3fs *S3FileSystem) Open(ctx context.Context, name string) (fs.File, error) {
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

func (s3fs *S3FileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (fs.File, error) {
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
	multipartUpload := NewMultipartUpload(ctx, client, bucket, s3fs.bucketKeyEnabled, key)
	if !doesNotExist {
		readSeeker = NewReadSeeker(
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

	return NewS3File(name, readSeeker, downloader, multipartUpload), nil
}

// func (s3fs *S3FileSystem) SyncDirectory(ctx context.Context, sourceDirectory string, destinationDirectory string, checkTimestamps bool, limit int, logger fs.Logger) (int, error) {
func (s3fs *S3FileSystem) SyncDirectory(ctx context.Context, input *fs.SyncDirectoryInput) (int, error) {

	// limit is zero
	if input.Limit == 0 {
		return 0, nil
	}

	sourceDirectoryEntries, err := s3fs.ReadDir(ctx, input.SourceDirectory)
	if err != nil {
		return 0, fmt.Errorf("error reading source directory %q: %w", input.SourceDirectory, err)
	}

	// wait group
	var wg errgroup.Group

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
				SourceDirectory:      sourceName,
				DestinationDirectory: destinationName,
				CheckTimestamps:      input.CheckTimestamps,
				Limit:                directoryLimit,
				Logger:               input.Logger,
				MaxThreads:           input.MaxThreads,
			})
			if err != nil {
				return 0, err
			}
			count += c
		} else {
			count += 1
			wg.Go(func() error {
				copyFile := false
				destinationFileInfo, err := s3fs.Stat(ctx, destinationName)
				if err != nil {
					if s3fs.IsNotExist(err) {
						copyFile = true
					} else {
						return fmt.Errorf("error stating destination %q: %w", destinationName, err)
					}
				} else {
					if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
						copyFile = true
					}
					if input.CheckTimestamps {
						if !fs.EqualTimestamp(sourceDirectoryEntry.ModTime(), destinationFileInfo.ModTime(), time.Second) {
							copyFile = true
						}
					}
				}
				if copyFile {
					err := s3fs.Copy(context.Background(), &fs.CopyInput{
						SourceName:      sourceName,
						DestinationName: destinationName,
						Parents:         true,
						Logger:          input.Logger,
					})
					if err != nil {
						return fmt.Errorf("error copying %q to %q: %w", sourceName, destinationName, err)
					}
				}
				return nil
			})
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

	if input.Logger != nil {
		input.Logger.Log("Synchronizing", map[string]interface{}{
			"src":     input.Source,
			"dst":     input.Destination,
			"threads": input.MaxThreads,
		})
	}

	sourceFileInfo, err := s3fs.Stat(ctx, input.Source)
	if err != nil {
		if s3fs.IsNotExist(err) {
			return 0, fmt.Errorf("source does not exist %q: %w", input.Source, err)
		}
		return 0, fmt.Errorf("error stating source %q: %w", input.Source, err)
	}

	if len(s3fs.bucket) == 0 {
		destinationBucket := strings.Split(strings.TrimPrefix(input.Destination, "/"), "/")[0]
		_, err := s3fs.Stat(ctx, destinationBucket)
		if err != nil {
			if s3fs.IsNotExist(err) {
				return 0, fmt.Errorf("bucket for destination does not exist %q", destinationBucket)
			}
			return 0, fmt.Errorf("error stating destination bucket %q", destinationBucket)
		}
	}

	// if source is a directory
	if sourceFileInfo.IsDir() {
		if _, err := s3fs.Stat(ctx, input.Destination); err != nil {
			if s3fs.IsNotExist(err) {
				if !input.Parents {
					return 0, fmt.Errorf("destination directory does not exist and parents is false: %q", input.Destination)
				}
			}
		}
		count, err := s3fs.SyncDirectory(ctx, &fs.SyncDirectoryInput{
			SourceDirectory:      input.Source,
			DestinationDirectory: input.Destination,
			CheckTimestamps:      input.CheckTimestamps,
			Limit:                input.Limit,
			Logger:               input.Logger,
			MaxThreads:           input.MaxThreads,
		})
		if err != nil {
			return 0, fmt.Errorf(
				"error syncing source directory %q to destination directory %q: %w",
				input.Source,
				input.Destination,
				err)
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
			if !fs.EqualTimestamp(sourceFileInfo.ModTime(), destinationFileInfo.ModTime(), time.Second) {
				copyFile = true
			}
		}
	}

	if copyFile {
		err = s3fs.Copy(ctx, &fs.CopyInput{
			SourceName:      input.Source,
			DestinationName: input.Destination,
			Parents:         input.Parents,
			Logger:          input.Logger,
		})
		if err != nil {
			return 0, fmt.Errorf("error copying %q to %q: %w", input.Source, input.Destination, err)
		}
		return 1, nil
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
