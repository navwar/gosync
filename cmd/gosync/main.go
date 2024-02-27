// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/navwar/gosync/pkg/fs"
	"github.com/navwar/gosync/pkg/lfs"
	"github.com/navwar/gosync/pkg/log"
	"github.com/navwar/gosync/pkg/s3fs"
	"github.com/navwar/gosync/pkg/ts"
)

const (
	GoSyncVersion = "0.0.1"
)

// AWS Flags
const (
	// Profile
	flagAWSPartition     = "aws-partition"
	flagAWSProfile       = "aws-profile"
	flagAWSDefaultRegion = "aws-default-region"
	flagAWSRegion        = "aws-region"
	// Credentials
	flagAWSAccessKeyID     = "aws-access-key-id"
	flagAWSSecretAccessKey = "aws-secret-access-key"
	flagAWSSessionToken    = "aws-session-token"
	flagAWSMFASerial       = "aws-mfa-serial"
	// Role
	flagAWSRoleARN            = "aws-role-arn"
	flagAWSAssumeRoleDuration = "aws-assume-role-duration"
	// Client
	flagAWSRetryMaxAttempts = "aws-retry-max-attempts"
	// TLS
	flagAWSInsecureSkipVerify = "aws-insecure-skip-verify"
	// Miscellaneous
	flagAWSS3Endpoint     = "aws-s3-endpoint"
	flagAWSS3UsePathStyle = "aws-s3-use-path-style"
	flagBucketKeyEnabled  = "aws-bucket-key-enabled"
)

// Sync Flags
const (
	// AWS Source Flags
	flagSourceAWSProfile         = "source-aws-profile"
	flagSourceAWSRegion          = "source-aws-region"
	flagSourceAWSS3Endpoint      = "source-aws-s3-endpoint"
	flagSourceAWSS3UsePathStyle  = "source-aws-s3-use-path-style"
	flagSourceAWSAccessKeyID     = "source-aws-access-key-id"
	flagSourceAWSSecretAccessKey = "source-aws-secret-access-key"
	flagSourceAWSSessionToken    = "source-aws-session-token"
	// AWS Destination Flags
	flagDestinationAWSProfile         = "destination-aws-profile"
	flagDestinationAWSRegion          = "destination-aws-region"
	flagDestinationAWSS3Endpoint      = "destination-aws-s3-endpoint"
	flagDestinationAWSS3UsePathStyle  = "destination-aws-s3-use-path-style"
	flagDestinationAWSAccessKeyID     = "destination-aws-access-key-id"
	flagDestinationAWSSecretAccessKey = "destination-aws-secret-access-key"
	flagDestinationAWSSessionToken    = "destination-aws-session-token"
)

// AWS Defaults
const (
	// Role
	DefaultAWSAssumeRoleDuration = 15 * time.Minute
	MinimumAWSAssumeRoleDuration = 15 * time.Minute
)

// Debug Flag
const (
	flagDebug = "debug"
)

// List Flags
const (
	flagAll                   = "all"
	flagRecursive             = "recursive"
	flagTimeLayout            = "time-layout"
	flagTimeZone              = "time-zone"
	flagHumanReadableFileSize = "human-readable-file-size"
	flagMagicNumber           = "magic-number"
	flagMaxPages              = "max-pages"
	flagMaxDirectoryEntries   = "max-directory-entries"
)

// List Defaults
const (
	DefaultFormat          = "text"
	DefaultTimestampFormat = "Jan 02 15:04"
)

// Sync Flags
const (
	flagDelete    = "delete"
	flagParents   = "parents"
	flagSyncLimit = "limit"
	//
	flagCheckTimestamps = "check-timestamps"
	flagThreads         = "threads"
	flagPartSize        = "part-size"
	//
	flagExclude            = "exclude"
	flagTimestampPrecision = "timestamp-precision"
)

// Sync Defaults
const (
	DefaultLimit              = -1
	DefaultPartSize           = 1_048_576 * 100 // 100 MiB
	DefaultTimestampPrecision = time.Second

	MinimumPartSize = 1_048_576 * 4 // 5 MiB
)

// Log Flags
const (
	flagLogPath            = "log-path"
	flagLogFormat          = "log-format"
	flagLogPerm            = "log-perm"
	flagLogClientSigning   = "log-client-signing"
	flagLogClientRequests  = "log-client-requests"
	flagLogClientResponses = "log-client-responses"
	flagLogClientRetries   = "log-client-retries"
)

// InitAWSFlags initializes the AWS flags.
func initAWSFlags(flag *pflag.FlagSet) {
	// Profile
	flag.String(flagAWSPartition, "aws", "AWS Partition")
	flag.String(flagAWSProfile, "default", "AWS Profile")
	flag.String(flagAWSDefaultRegion, "", "AWS Default Region")
	flag.String(flagAWSRegion, "", "AWS Region (overrides default region)")
	// Credentials
	flag.String(flagAWSAccessKeyID, "", "AWS Access Key ID")
	flag.String(flagAWSSecretAccessKey, "", "AWS Secret Access Key")
	flag.String(flagAWSSessionToken, "", "AWS Session Token")
	flag.String(flagAWSMFASerial, "", "AWS MFA Serial")
	// Role
	flag.String(flagAWSRoleARN, "", "AWS Role ARN")
	flag.Duration(flagAWSAssumeRoleDuration, DefaultAWSAssumeRoleDuration, "Expiry duration of the STS credentials for assuming a role.")
	// Client
	flag.Int(flagAWSRetryMaxAttempts, 5, "the maximum number attempts an AWS API client will call an operation that fails with a retryable error.")
	// TLS
	flag.Bool(flagAWSInsecureSkipVerify, false, "Skip verification of AWS TLS certificate")
	// Misceallenous
	flag.String(flagAWSS3Endpoint, "", "AWS S3 Endpoint URL")
	flag.Bool(flagAWSS3UsePathStyle, false, "Use path-style addressing (default is to use virtual-host-style addressing)")
	flag.Bool(flagBucketKeyEnabled, false, "bucket key enabled")
}

func initDebugFlags(flag *pflag.FlagSet) {
	flag.BoolP(flagDebug, "d", false, "print debug messages")
}

func initListFlags(flag *pflag.FlagSet) {
	flag.BoolP(flagAll, "a", false, "Include directory entries whose names begin with a dot (‘.’).")
	flag.StringP(flagLogFormat, "f", DefaultFormat, "output log format.  Either jsonl or text.")
	flag.Bool(flagMagicNumber, false, "include magic number in output as hex")
	flag.StringP(flagTimeLayout, "t", "Default", "the layout to use for file timestamps.  Use go layout format, or the name of a layout.  Use gosync layouts to show all named layouts.")
	flag.StringP(flagTimeZone, "z", "Local", "the timezone to use for file timestamps")
	flag.Bool(flagHumanReadableFileSize, false, "display file sizes in human-readable format")
	flag.BoolP(flagRecursive, "r", false, "recursively list sub-directories breadth-first")
	flag.Int(flagMaxDirectoryEntries, -1, "maximum directory entries for each page returned by the filesystem")
	flag.Int(flagMaxPages, -1, "maximum number of pages to return from the filesystem when reading a directory")
}

func initSyncFlags(flag *pflag.FlagSet) {
	flag.Bool(flagDelete, false, "delete files at destination that do not exist at source")
	flag.BoolP(flagParents, "p", false, "create parent directories for destination if they do not exist")
	flag.Int(flagSyncLimit, DefaultLimit, "limit number of files copied")
	flag.Bool(flagCheckTimestamps, false, "check timestamps are equal")
	flag.Int(flagThreads, 1, "maximum number of parallel threads")
	flag.Int(flagPartSize, DefaultPartSize, fmt.Sprintf("size of parts in bytes when transferring to S3 (minimum %d)", MinimumPartSize))
	flag.StringP(flagExclude, "e", "", "a colon-separated list of paths to exclude with support for wildcards, e.g, path, *path, path*, or *path*.")
	flag.Duration(flagTimestampPrecision, DefaultTimestampPrecision, "precision to use when checking timestamps")
	// AWS Source Flags
	flag.String(flagSourceAWSProfile, "default", "AWS Profile for source")
	flag.String(flagSourceAWSRegion, "", "AWS Region for source")
	flag.String(flagSourceAWSS3Endpoint, "", "AWS S3 Endpoint URL for source")
	flag.Bool(flagSourceAWSS3UsePathStyle, false, "Use path-style addressing (default is to use virtual-host-style addressing) for source")
	flag.String(flagSourceAWSAccessKeyID, "", "AWS Access Key ID for source")
	flag.String(flagSourceAWSSecretAccessKey, "", "AWS Secret Access Key for source")
	flag.String(flagSourceAWSSessionToken, "", "AWS Session Token for source")
	// AWS Destination Flags
	flag.String(flagDestinationAWSProfile, "default", "AWS Profile for destination")
	flag.String(flagDestinationAWSRegion, "", "AWS Region for destination")
	flag.String(flagDestinationAWSS3Endpoint, "", "AWS S3 Endpoint URL for destination")
	flag.Bool(flagDestinationAWSS3UsePathStyle, false, "Use path-style addressing (default is to use virtual-host-style addressing) for destination")
	flag.String(flagDestinationAWSAccessKeyID, "", "AWS Access Key ID for destination")
	flag.String(flagDestinationAWSSecretAccessKey, "", "AWS Secret Access Key for destination")
	flag.String(flagDestinationAWSSessionToken, "", "AWS Session Token for destination")
}

func initLogFlags(flag *pflag.FlagSet) {
	flag.String(flagLogPath, "-", "path to the log output.  Defaults to the operating system's stdout device.")
	flag.String(flagLogPerm, "0600", "file permissions for log output file as unix file mode.")
	flag.Bool(flagLogClientSigning, false, "log AWS client signature requests")
	flag.Bool(flagLogClientRequests, false, "log AWS client requests")
	flag.Bool(flagLogClientResponses, false, "log AWS client responses")
	flag.Bool(flagLogClientRetries, false, "log AWS client retries")
}

func initListCommandFlags(flag *pflag.FlagSet) {
	initDebugFlags(flag)
	initAWSFlags(flag)
	initListFlags(flag)
	initLogFlags(flag)
}

func initSyncCommandFlags(flag *pflag.FlagSet) {
	initDebugFlags(flag)
	initAWSFlags(flag)
	initListFlags(flag)
	initSyncFlags(flag)
	initLogFlags(flag)
}

func initViper(cmd *cobra.Command) (*viper.Viper, error) {
	v := viper.New()
	err := v.BindPFlags(cmd.Flags())
	if err != nil {
		return v, fmt.Errorf("error binding flag set to viper: %w", err)
	}
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv() // set environment variables to overwrite config
	return v, nil
}

func checkAWSConfig(v *viper.Viper, args []string) error {
	assumeRoleDuration := v.GetDuration(flagAWSAssumeRoleDuration)
	if assumeRoleDuration < 15*time.Minute {
		return fmt.Errorf(
			"%q value %q is invalid, expecting value greater than or equal to %q",
			flagAWSAssumeRoleDuration,
			assumeRoleDuration,
			MinimumAWSAssumeRoleDuration)
	}
	return nil
}

func checkLogConfig(v *viper.Viper, args []string) error {
	logPath := v.GetString(flagLogPath)
	if len(logPath) == 0 {
		return fmt.Errorf("log path is missing")
	}
	logPerm := v.GetString(flagLogPerm)
	if len(logPerm) == 0 {
		return fmt.Errorf("log perm is missing")
	}
	_, err := strconv.ParseUint(logPerm, 8, 32)
	if err != nil {
		return fmt.Errorf("invalid format for log perm: %s", logPerm)
	}
	return nil
}

func checkListConfig(v *viper.Viper, args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("expecting at most 1 positional argument for source, but found %d arguments", len(args))
	}
	if err := checkAWSConfig(v, args); err != nil {
		return fmt.Errorf("error with AWS configuration: %w", err)
	}
	if err := checkLogConfig(v, args); err != nil {
		return fmt.Errorf("error with log configuration: %w", err)
	}
	return nil
}

func checkSyncConfig(v *viper.Viper, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("expecting 2 positional arguments for source and destination, but found %d arguments", len(args))
	}

	if strings.HasPrefix(args[0], "s3://") && strings.HasPrefix(args[1], "s3://") {
		sourceEndpoint := v.GetString(flagSourceAWSS3Endpoint)
		if len(sourceEndpoint) == 0 {
			sourceEndpoint = v.GetString(flagAWSS3Endpoint)
		}
		destinationEndpoint := v.GetString(flagDestinationAWSS3Endpoint)
		if len(destinationEndpoint) == 0 {
			destinationEndpoint = v.GetString(flagAWSS3Endpoint)
		}
		if sourceEndpoint == destinationEndpoint {
			if err := s3fs.Check(args[0][len("s3://"):], args[1][len("s3://"):]); err != nil {
				return err
			}
		}
	} else {
		sourcePath := args[0]
		if strings.HasPrefix(sourcePath, "file://") {
			sourcePath = args[0][len("file://"):]
		}
		destinationPath := args[1]
		if strings.HasPrefix(destinationPath, "file://") {
			destinationPath = args[1][len("file://"):]
		}
		// check that source and destination must be different
		if args[0] == args[1] {
			return fmt.Errorf("source and destination must be different: %q", args[0])
		}
		// check for cycle errors
		if err := lfs.Check(sourcePath, destinationPath); err != nil {
			return err
		}
	}
	if err := checkAWSConfig(v, args); err != nil {
		return fmt.Errorf("error with AWS configuration: %w", err)
	}
	if err := checkLogConfig(v, args); err != nil {
		return fmt.Errorf("error with log configuration: %w", err)
	}
	if partSize := v.GetInt(flagPartSize); partSize < MinimumPartSize {
		return fmt.Errorf("part size %d is less than the minimum part size %d", partSize, MinimumPartSize)
	}
	if threads := v.GetInt(flagThreads); threads == 0 {
		return errors.New("threads cannot be zero")
	}
	if checkTimestamps := v.GetBool(flagCheckTimestamps); checkTimestamps {
		if strings.HasPrefix(args[1], "s3://") {
			return fmt.Errorf("checking timestamps is incompatible with s3 destinations, as timestamps cannot be preserved")
		}
	}
	return nil
}

type InitS3ClientInput struct {
	Profile   string
	Partition string
	Region    string
	// AWS Client
	Endpoint           string
	InsecureSkipVerify bool
	RetryMaxAttempts   int
	UsePathStyle       bool
	// AWS Credentials
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// Client Log Mode
	LogClientSigning   bool
	LogClientRetries   bool
	LogClientRequests  bool
	LogClientResponses bool
}

func InitS3Client(ctx context.Context, input *InitS3ClientInput) *s3.Client {
	clientLogMode := aws.ClientLogMode(0)
	if input.LogClientSigning {
		clientLogMode |= aws.LogSigning
	}
	if input.LogClientRetries {
		clientLogMode |= aws.LogRetries
	}
	if input.LogClientRequests {
		clientLogMode |= aws.LogRequest
	}
	if input.LogClientResponses {
		clientLogMode |= aws.LogResponse
	}

	c := aws.Config{
		ClientLogMode:    clientLogMode,
		RetryMaxAttempts: input.RetryMaxAttempts,
		Region:           input.Region,
		Logger:           log.NewClientLogger(os.Stdout),
	}

	if len(input.AccessKeyID) > 0 && len(input.SecretAccessKey) > 0 {
		c.Credentials = credentials.NewStaticCredentialsProvider(
			input.AccessKeyID,
			input.SecretAccessKey,
			input.SessionToken)
	} else {
		sharedConfig, err := config.LoadSharedConfigProfile(ctx, input.Profile)
		if err == nil {
			c.Credentials = credentials.NewStaticCredentialsProvider(
				sharedConfig.Credentials.AccessKeyID,
				sharedConfig.Credentials.SecretAccessKey,
				"")
		}
	}

	if input.InsecureSkipVerify {
		c.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	client := s3.NewFromConfig(c, func(o *s3.Options) {
		o.UsePathStyle = input.UsePathStyle
		if len(input.Endpoint) > 0 {
			o.BaseEndpoint = aws.String(input.Endpoint)
		}
	})

	return client
}

type InitFileSystemInput struct {
	Viper               *viper.Viper
	Root                string
	Profile             string
	Partition           string
	DefaultRegion       string
	MaxDirectoryEntries int
	MaxPages            int
	BucketKeyEnabled    bool
	SourceBucket        string
	DestinationBucket   string
	// AWS Client
	Endpoint           string
	InsecureSkipVerify bool
	RetryMaxAttempts   int
	UsePathStyle       bool
	PartSize           int
	// AWS Credentials
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// Client Log Mode
	LogClientSigning   bool
	LogClientRetries   bool
	LogClientRequests  bool
	LogClientResponses bool
}

func InitFileSystem(ctx context.Context, input *InitFileSystemInput) fs.FileSystem {

	if strings.HasPrefix(input.Root, "file://") {
		return lfs.NewLocalFileSystem(input.Root[len("file://"):])
	}

	if strings.HasPrefix(input.Root, "s3://") {
		//
		// List all buckets in accounts and store creation date
		//

		clients := map[string]*s3.Client{}

		//
		// Initialize Default Client
		//

		clients[input.DefaultRegion] = InitS3Client(ctx, &InitS3ClientInput{
			Profile:   input.Profile,
			Partition: input.Partition,
			Region:    input.DefaultRegion,
			// AWS Client
			Endpoint:           input.Endpoint,
			InsecureSkipVerify: input.InsecureSkipVerify,
			RetryMaxAttempts:   input.RetryMaxAttempts,
			UsePathStyle:       input.UsePathStyle,
			// AWS Credentials
			AccessKeyID:     input.AccessKeyID,
			SecretAccessKey: input.SecretAccessKey,
			SessionToken:    input.SessionToken,
			// Client Mode
			LogClientSigning:   input.LogClientSigning,
			LogClientRetries:   input.LogClientRetries,
			LogClientRequests:  input.LogClientRequests,
			LogClientResponses: input.LogClientResponses,
		})

		//

		bucketCreationDates := map[string]time.Time{}
		bucketRegions := map[string]string{}

		//
		// List Buckets
		//

		listBucketsOutput, err := clients[input.DefaultRegion].ListBuckets(ctx, &s3.ListBucketsInput{})
		if err == nil {
			// if root is s3:// and don't know enough to filter by source or destination
			if input.Root == "s3://" && input.SourceBucket == "" && input.DestinationBucket == "" {
				// do not filter
				for _, b := range listBucketsOutput.Buckets {
					bucketCreationDates[aws.ToString(b.Name)] = aws.ToTime(b.CreationDate)
				}
			} else {
				for _, b := range listBucketsOutput.Buckets {
					if bucketName := aws.ToString(b.Name); bucketName == input.SourceBucket || bucketName == input.DestinationBucket {
						bucketCreationDates[bucketName] = aws.ToTime(b.CreationDate)
					}
				}
			}
		}

		//
		// If root is the account
		//

		if input.Root == "s3://" {
			//
			// Get Region for each Bucket
			//
			for bucketName := range bucketCreationDates {
				getBucketLocationOutput, getBucketLocationError := clients[input.DefaultRegion].GetBucketLocation(ctx, &s3.GetBucketLocationInput{
					Bucket: aws.String(bucketName),
				})
				if getBucketLocationError == nil {
					if locationConstraint := string(getBucketLocationOutput.LocationConstraint); len(locationConstraint) > 0 {
						bucketRegions[bucketName] = locationConstraint
					} else {
						bucketRegions[bucketName] = "us-east-1"
					}
				}
			}

			//
			// Get Client for each region with a bucket
			//

			for _, bucketRegion := range bucketRegions {
				if _, ok := clients[bucketRegion]; !ok {
					clients[bucketRegion] = InitS3Client(ctx, &InitS3ClientInput{
						Profile:   input.Profile,
						Partition: input.Partition,
						Region:    bucketRegion,
						// AWS Client
						Endpoint:           input.Endpoint,
						InsecureSkipVerify: input.InsecureSkipVerify,
						RetryMaxAttempts:   input.RetryMaxAttempts,
						UsePathStyle:       input.UsePathStyle,
						// AWS Credentials
						AccessKeyID:     input.AccessKeyID,
						SecretAccessKey: input.SecretAccessKey,
						SessionToken:    input.SessionToken,
						// Client Mode
						LogClientSigning:   input.LogClientSigning,
						LogClientRetries:   input.LogClientRetries,
						LogClientRequests:  input.LogClientRequests,
						LogClientResponses: input.LogClientResponses,
					})
				}
			}

			return s3fs.NewS3FileSystem(
				input.DefaultRegion,
				"",
				"",
				clients,
				bucketRegions,
				bucketCreationDates,
				input.MaxDirectoryEntries,
				input.MaxPages,
				input.BucketKeyEnabled,
				input.PartSize)
		}

		//
		// If root is a bucket
		//

		rootParts := strings.Split(input.Root[len("s3://"):], "/")
		bucketName := rootParts[0]
		prefix := strings.Join(rootParts[1:], "/")

		getBucketLocationOutput, err := clients[input.DefaultRegion].GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			if locationConstraint := string(getBucketLocationOutput.LocationConstraint); len(locationConstraint) > 0 {
				bucketRegions[bucketName] = locationConstraint
			} else {
				bucketRegions[bucketName] = "us-east-1"
			}
		}

		//
		// Get Client for the region of the bucket
		//

		if bucketRegion, ok := bucketRegions[bucketName]; ok {
			clients[bucketRegion] = InitS3Client(ctx, &InitS3ClientInput{
				Profile:   input.Profile,
				Partition: input.Partition,
				Region:    bucketRegion,
				// AWS Client
				Endpoint:           input.Endpoint,
				InsecureSkipVerify: input.InsecureSkipVerify,
				RetryMaxAttempts:   input.RetryMaxAttempts,
				UsePathStyle:       input.UsePathStyle,
				// AWS Credentials
				AccessKeyID:     input.AccessKeyID,
				SecretAccessKey: input.SecretAccessKey,
				SessionToken:    input.SessionToken,
				// Client Mode
				LogClientSigning:   input.LogClientSigning,
				LogClientRetries:   input.LogClientRetries,
				LogClientRequests:  input.LogClientRequests,
				LogClientResponses: input.LogClientResponses,
			})
		} else {
			// It GetBucketLocation is not allowed, assume that the default region is the region containing the bucket
			clients[input.DefaultRegion] = InitS3Client(ctx, &InitS3ClientInput{
				Profile:   input.Profile,
				Partition: input.Partition,
				Region:    input.DefaultRegion,
				// AWS Client
				Endpoint:           input.Endpoint,
				InsecureSkipVerify: input.InsecureSkipVerify,
				RetryMaxAttempts:   input.RetryMaxAttempts,
				UsePathStyle:       input.UsePathStyle,
				// AWS Credentials
				AccessKeyID:     input.AccessKeyID,
				SecretAccessKey: input.SecretAccessKey,
				SessionToken:    input.SessionToken,
				// Client Mode
				LogClientSigning:   input.LogClientSigning,
				LogClientRetries:   input.LogClientRetries,
				LogClientRequests:  input.LogClientRequests,
				LogClientResponses: input.LogClientResponses,
			})
		}

		return s3fs.NewS3FileSystem(
			input.DefaultRegion,
			bucketName,
			prefix,
			clients,
			bucketRegions,
			bucketCreationDates,
			input.MaxDirectoryEntries,
			input.MaxPages,
			input.BucketKeyEnabled,
			input.PartSize)
	}

	return lfs.NewLocalFileSystem(input.Root)
}

func initLogger(path string, perm string) (*log.SimpleLogger, error) {

	if path == os.DevNull {
		return log.NewSimpleLogger(io.Discard), nil
	}

	if path == "-" {
		return log.NewSimpleLogger(os.Stdout), nil
	}

	fileMode := os.FileMode(0600)

	if len(perm) > 0 {
		fm, err := strconv.ParseUint(perm, 8, 32)
		if err != nil {
			return nil, fmt.Errorf("error parsing file permissions for log file from %q", perm)
		}
		fileMode = os.FileMode(fm)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, fileMode)
	if err != nil {
		return nil, fmt.Errorf("error opening log file %q: %w", path, err)
	}

	return log.NewSimpleLogger(f), nil
}

func formatHumanReadableFileSize(size int64) string {
	str := ""
	if size <= int64(math.Pow(2, 10)) {
		str = fmt.Sprintf("%dB", size)
	} else if size <= int64(math.Pow(2, 20)) {
		f := float64(size) / math.Pow(2, 10)
		if f > 10 {
			str = fmt.Sprintf("%.0fK", f)
		} else {
			str = fmt.Sprintf("%.1fK", f)
		}
	} else if size <= int64(math.Pow(2, 30)) {
		str = fmt.Sprintf("%.0fM", float64(size)/math.Pow(2, 20))
	} else {
		str = fmt.Sprintf("%.0fG", float64(size)/math.Pow(2, 30))
	}
	return fmt.Sprintf("%5s", str)
}

func main() {
	rootCommand := &cobra.Command{
		Use:                   `gosync [flags]`,
		DisableFlagsInUseLine: true,
		Short: strings.Join([]string{
			"gosync is a simple command line program for synchronizing two directories specified by URI.",
			"gosync schemes returns the currently supported schemes.",
			"Local files are specified using the \"file://\" scheme or a path without a scheme.",
			"S3 files are specified using the \"s3://\" scheme.",
		}, "\n"),
	}

	layoutsCommand := &cobra.Command{
		Use:                   `layouts`,
		DisableFlagsInUseLine: true,
		Short:                 "show supported timestamp layouts",
		SilenceErrors:         true,
		SilenceUsage:          true,
		RunE: func(cmd *cobra.Command, args []string) error {
			names := make([]string, 0, len(ts.NamedLayouts))
			for name := range ts.NamedLayouts {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				fmt.Printf("%s: %s\n", name, ts.NamedLayouts[name])
			}
			return nil
		},
	}

	listCommand := &cobra.Command{
		Use:                   "list URI",
		DisableFlagsInUseLine: true,
		Short:                 "list",
		Long:                  "list the directory at the URI",
		SilenceErrors:         true,
		SilenceUsage:          true,
		RunE: func(cmd *cobra.Command, args []string) error {

			ctx := cmd.Context()

			v, err := initViper(cmd)
			if err != nil {
				return fmt.Errorf("error initializing viper: %w", err)
			}

			if errConfig := checkListConfig(v, args); errConfig != nil {
				return errConfig
			}

			debug := v.GetBool(flagDebug)

			logger, err := initLogger(v.GetString(flagLogPath), v.GetString(flagLogPerm))
			if err != nil {
				return fmt.Errorf("error initializing logger: %w", err)
			}

			uri := "."
			if len(args) == 1 {
				uri = args[0]
			}

			if uri == "." {
				cwd, getwdError := os.Getwd()
				if getwdError != nil {
					_ = logger.Log("Error getting current working direcetory", map[string]interface{}{
						"err": getwdError.Error(),
					})
					os.Exit(1)
				}
				uri = cwd
			}

			profile := v.GetString(flagAWSProfile)
			if len(profile) == 0 {
				profile = "default"
			}

			partition := v.GetString(flagAWSPartition)
			if len(partition) == 0 {
				partition = "aws"
			}

			region := v.GetString(flagAWSRegion)
			if len(region) == 0 {
				if defaultRegion := v.GetString(flagAWSDefaultRegion); len(defaultRegion) > 0 {
					region = defaultRegion
				}
			}

			// if neither region nor default region is specified
			if len(region) == 0 {
				sharedConfig, loadSharedConfigProfileError := config.LoadSharedConfigProfile(ctx, profile)
				if loadSharedConfigProfileError == nil {
					region = sharedConfig.Region
				}
			}

			maxDirectoryEntries := v.GetInt(flagMaxDirectoryEntries)
			maxPages := v.GetInt(flagMaxPages)

			bucket := ""
			if uri != "s3://" && strings.HasPrefix(uri, "s3://") {
				bucket = s3fs.Split(uri[len("s3://"):])[0]
			}

			// create file system
			if debug {
				fields := map[string]interface{}{
					"root": uri,
				}
				if e := v.GetString(flagAWSS3Endpoint); len(e) > 0 {
					fields["endpoint"] = e
				}
				_ = logger.Log("Creating filesystem", fields)
			}

			fileSystem := InitFileSystem(ctx, &InitFileSystemInput{
				Viper:               v,
				Root:                uri,
				Profile:             profile,
				Partition:           partition,
				DefaultRegion:       region,
				MaxDirectoryEntries: maxDirectoryEntries,
				MaxPages:            maxPages,
				BucketKeyEnabled:    false, // not applicable
				SourceBucket:        bucket,
				DestinationBucket:   "", // not applicable
				// AWS Client
				Endpoint:           v.GetString(flagAWSS3Endpoint),
				InsecureSkipVerify: v.GetBool(flagAWSInsecureSkipVerify),
				UsePathStyle:       v.GetBool(flagAWSS3UsePathStyle),
				RetryMaxAttempts:   v.GetInt(flagAWSRetryMaxAttempts),
				PartSize:           -1, // not applicable
				// AWS Credentials
				AccessKeyID:     v.GetString(flagAWSAccessKeyID),
				SecretAccessKey: v.GetString(flagAWSSecretAccessKey),
				SessionToken:    v.GetString(flagAWSSessionToken),
				// Client Mode
				LogClientSigning:   v.GetBool(flagLogClientSigning),
				LogClientRetries:   v.GetBool(flagLogClientRetries),
				LogClientRequests:  v.GetBool(flagLogClientRequests),
				LogClientResponses: v.GetBool(flagLogClientResponses),
			})

			// func initFileSystem(ctx context.Context, v *viper.Viper, root string, profile string, partition string, defaultRegion string,
			// maxDirectoryEntries int, maxPages int, bucketKeyEnabled bool, sourceBucket string, destinationBucket string) fs.FileSystem {

			//
			// List
			//

			recursive := v.GetBool(flagRecursive)

			directoryEntries, err := fileSystem.ReadDir(ctx, "/", recursive)
			if err != nil {
				_ = logger.Log("Error listing", map[string]interface{}{
					"uri": uri,
					"err": err.Error(),
				})
				os.Exit(1)
			}

			logFormat := v.GetString(flagLogFormat)
			magicNumber := v.GetBool(flagMagicNumber)
			humanReadableFileSize := v.GetBool(flagHumanReadableFileSize)
			timeLayout := ts.ParseLayout(v.GetString(flagTimeLayout))
			timeZone, err := ts.ParseLocation(v.GetString(flagTimeZone))
			if err != nil {
				return fmt.Errorf("error parsing time zone location %q: %w", v.GetString(flagTimeZone), err)
			}

			all := v.GetBool(flagAll)

			magicNumbers := make([][]byte, len(directoryEntries))
			if magicNumber {
				names := make([]string, 0, len(directoryEntries))
				for _, de := range directoryEntries {
					if de.IsDir() {
						names = append(names, "")
					} else {
						names = append(names, de.Name())
					}
				}
				mn, err := fileSystem.MagicNumbers(ctx, names, -1)
				if err != nil {
					return fmt.Errorf("error retrieving magic numbers: %w", err)
				}
				magicNumbers = mn
			}

			switch logFormat {
			case "text":
				if humanReadableFileSize {
					if magicNumber {
						_, _ = fmt.Fprintf(os.Stdout, "%s %s %s %s %s\n",
							"type",
							"magic",
							" size",
							fmt.Sprintf("%"+strconv.Itoa(len(timeLayout))+"s", "modified"),
							"name",
						)
						for i, de := range directoryEntries {
							name := de.Name()
							if all || !strings.HasPrefix(name, ".") {
								fileType := "file"
								if de.IsDir() {
									fileType = " dir"
								}
								mn := "     "
								if !de.IsDir() {
									mn = fmt.Sprintf(" %x", magicNumbers[i])
								}
								_, _ = fmt.Fprintf(os.Stdout, "%s %s %s %s %s\n",
									fileType,
									mn,
									formatHumanReadableFileSize(de.Size()),
									timeLayout.Format(de.ModTime().In(timeZone)),
									name)
							}
						}
					} else {
						_, _ = fmt.Fprintf(os.Stdout, "%s %s %s %s\n",
							"type",
							" size",
							fmt.Sprintf("%"+strconv.Itoa(len(timeLayout))+"s", "modified"),
							"name",
						)
						for _, de := range directoryEntries {
							name := de.Name()
							if all || !strings.HasPrefix(name, ".") {
								fileType := "file"
								if de.IsDir() {
									fileType = " dir"
								}
								_, _ = fmt.Fprintf(os.Stdout, "%s %s %s %s\n",
									fileType,
									formatHumanReadableFileSize(de.Size()),
									timeLayout.Format(de.ModTime().In(timeZone)),
									name)
							}
						}
					}
				} else {
					if magicNumber {
						maxFileSize := int64(0)
						for _, de := range directoryEntries {
							if name := de.Name(); all || !strings.HasPrefix(name, ".") {
								if size := de.Size(); size > maxFileSize {
									maxFileSize = de.Size()
								}
							}
						}
						spaces := len(fmt.Sprintf("%d", maxFileSize))

						fmt.Println("spaces:", spaces)

						_, _ = fmt.Fprintf(os.Stdout, "%s %s %s %s %s\n",
							"type",
							"magic",
							fmt.Sprintf("%"+strconv.Itoa(int(spaces))+"s", "size"),
							fmt.Sprintf("%"+strconv.Itoa(len(timeLayout))+"s", "modified"),
							"name",
						)

						for i, de := range directoryEntries {
							if name := de.Name(); all || !strings.HasPrefix(name, ".") {
								fileType := "file"
								if de.IsDir() {
									fileType = " dir"
								}
								mn := "     "
								if !de.IsDir() {
									mn = fmt.Sprintf(" %x", magicNumbers[i])
								}
								_, _ = fmt.Fprintf(os.Stdout, "%s %s %s %s %s\n",
									fileType,
									mn,
									fmt.Sprintf("%"+strconv.Itoa(int(spaces))+"d", de.Size()),
									timeLayout.Format(de.ModTime().In(timeZone)),
									name)
							}
						}
					} else {
						maxFileSize := int64(0)
						for _, de := range directoryEntries {
							if name := de.Name(); all || !strings.HasPrefix(name, ".") {
								if size := de.Size(); size > maxFileSize {
									maxFileSize = de.Size()
								}
							}
						}
						spaces := len(fmt.Sprintf("%d", maxFileSize))

						_, _ = fmt.Fprintf(os.Stdout, "%s %s %s %s\n",
							"type",
							fmt.Sprintf("%"+strconv.Itoa(int(spaces))+"s", "size"),
							fmt.Sprintf("%"+strconv.Itoa(len(timeLayout))+"s", "modified"),
							"name",
						)

						for _, de := range directoryEntries {
							if name := de.Name(); all || !strings.HasPrefix(name, ".") {
								fileType := "file"
								if de.IsDir() {
									fileType = " dir"
								}
								_, _ = fmt.Fprintf(os.Stdout, "%s %s %s %s\n",
									fileType,
									fmt.Sprintf("%"+strconv.Itoa(int(spaces))+"d", de.Size()),
									timeLayout.Format(de.ModTime().In(timeZone)),
									name)
							}
						}
					}
				}
			case "jsonl":
				encoder := json.NewEncoder(os.Stdout)
				for i, de := range directoryEntries {
					m := map[string]any{}
					m["name"] = de.Name()
					m["mod_time"] = timeLayout.Format(de.ModTime().In(timeZone))
					if humanReadableFileSize {
						m["size"] = strings.TrimSpace(formatHumanReadableFileSize(de.Size()))
					} else {
						m["size"] = de.Size()
					}
					if de.IsDir() {
						m["type"] = " dir"
					} else {
						m["type"] = "file"
					}
					if magicNumber {
						if de.IsDir() {
							m["magic_number"] = ""
						} else {
							m["magic_number"] = fmt.Sprintf("%x", magicNumbers[i])
						}
					}
					err := encoder.Encode(m)
					if err != nil {
						return fmt.Errorf("error encoding directory entry %#v: %W", de, err)
					}
				}
			default:
				_ = logger.Log("Unknown log format", map[string]interface{}{
					"format": logFormat,
				})
				os.Exit(1)
			}

			return nil

		},
	}
	initListCommandFlags(listCommand.Flags())

	syncCommand := &cobra.Command{
		Use:                   "sync SOURCE DESTINATION",
		DisableFlagsInUseLine: true,
		Short:                 "sync",
		Long:                  "synchronize source and destination",
		SilenceErrors:         true,
		SilenceUsage:          true,
		RunE: func(cmd *cobra.Command, args []string) error {

			ctx := cmd.Context()

			v, err := initViper(cmd)
			if err != nil {
				return fmt.Errorf("error initializing viper: %w", err)
			}

			if errConfig := checkSyncConfig(v, args); errConfig != nil {
				return errConfig
			}

			debug := v.GetBool(flagDebug)

			logger, err := initLogger(v.GetString(flagLogPath), v.GetString(flagLogPerm))
			if err != nil {
				return fmt.Errorf("error initializing logger: %w", err)
			}

			sourceURI := args[0]
			destinationURI := args[1]

			sourceProfile := v.GetString(flagSourceAWSProfile)
			if len(sourceProfile) == 0 {
				sourceProfile = v.GetString(flagAWSProfile)
				if len(sourceProfile) == 0 {
					sourceProfile = "default"
				}
			}

			destinationProfile := v.GetString(flagDestinationAWSProfile)
			if len(destinationProfile) == 0 {
				destinationProfile = v.GetString(flagAWSProfile)
				if len(destinationProfile) == 0 {
					destinationProfile = "default"
				}
			}

			partition := v.GetString(flagAWSPartition)
			if len(partition) == 0 {
				partition = "aws"
			}

			sourceRegion := v.GetString(flagSourceAWSRegion)
			if len(sourceRegion) == 0 {
				sourceRegion = v.GetString(flagAWSRegion)
				if len(sourceRegion) == 0 {
					sourceRegion = v.GetString(flagAWSDefaultRegion)
					if len(sourceRegion) == 0 {
						sharedConfig, loadSharedConfigProfileError := config.LoadSharedConfigProfile(ctx, sourceProfile)
						if loadSharedConfigProfileError == nil {
							sourceRegion = sharedConfig.Region
						}
					}
				}
			}

			destinationRegion := v.GetString(flagDestinationAWSRegion)
			if len(destinationRegion) == 0 {
				destinationRegion = v.GetString(flagAWSRegion)
				if len(destinationRegion) == 0 {
					destinationRegion = v.GetString(flagAWSDefaultRegion)
					if len(destinationRegion) == 0 {
						sharedConfig, loadSharedConfigProfileError := config.LoadSharedConfigProfile(ctx, destinationProfile)
						if loadSharedConfigProfileError == nil {
							destinationRegion = sharedConfig.Region
						}
					}
				}
			}

			sourceEndpoint := v.GetString(flagSourceAWSS3Endpoint)
			if len(sourceEndpoint) == 0 {
				sourceEndpoint = v.GetString(flagAWSS3Endpoint)
			}

			destinationEndpoint := v.GetString(flagDestinationAWSS3Endpoint)
			if len(destinationEndpoint) == 0 {
				destinationEndpoint = v.GetString(flagAWSS3Endpoint)
			}

			sourceUsePathStyle := v.GetBool(flagSourceAWSS3UsePathStyle) || v.GetBool(flagAWSS3UsePathStyle)
			destinationUsePathStyle := v.GetBool(flagDestinationAWSS3UsePathStyle) || v.GetBool(flagAWSS3UsePathStyle)

			syncDelete := v.GetBool(flagDelete)
			maxDirectoryEntries := v.GetInt(flagMaxDirectoryEntries)
			maxPages := v.GetInt(flagMaxPages)
			threads := v.GetInt(flagThreads)
			if threads == -1 {
				threads = runtime.NumCPU()
			}
			syncLimit := v.GetInt(flagSyncLimit)
			syncExclude := []string{}
			if syncExcludeString := v.GetString(flagExclude); len(syncExcludeString) > 0 {
				syncExclude = strings.Split(syncExcludeString, ":")
			}
			syncParents := v.GetBool(flagParents)
			bucketKeyEnabled := v.GetBool(flagBucketKeyEnabled)
			checkTimestamps := v.GetBool(flagCheckTimestamps)
			timestampPrecision := v.GetDuration(flagTimestampPrecision)
			// AWS Credentials
			//accessKeyID := v.GetString(flagAWSAccessKeyID)
			//secretAccessKey := v.GetString(flagAWSSecretAccessKey)
			//sessionToken := v.GetString(flagAWSSessionToken)
			// Source Credentials
			sourceAccessKeyID := v.GetString(flagSourceAWSAccessKeyID)
			if len(sourceAccessKeyID) == 0 {
				sourceAccessKeyID = v.GetString(flagAWSAccessKeyID)
			}
			sourceSecretAccessKey := v.GetString(flagSourceAWSSecretAccessKey)
			if len(sourceSecretAccessKey) == 0 {
				sourceSecretAccessKey = v.GetString(flagAWSSecretAccessKey)
			}
			sourceSessionToken := v.GetString(flagSourceAWSSessionToken)
			if len(sourceSessionToken) == 0 {
				sourceSessionToken = v.GetString(flagAWSSessionToken)
			}
			// Destination Credentials
			destinationAccessKeyID := v.GetString(flagDestinationAWSAccessKeyID)
			if len(destinationAccessKeyID) == 0 {
				destinationAccessKeyID = v.GetString(flagAWSAccessKeyID)
			}
			destinationSecretAccessKey := v.GetString(flagDestinationAWSSecretAccessKey)
			if len(destinationSecretAccessKey) == 0 {
				destinationSecretAccessKey = v.GetString(flagAWSSecretAccessKey)
			}
			destinationSessionToken := v.GetString(flagDestinationAWSSessionToken)
			if len(destinationSessionToken) == 0 {
				destinationSessionToken = v.GetString(flagAWSSessionToken)
			}
			// Client Log
			logClientSigning := v.GetBool(flagLogClientSigning)
			logClientRetries := v.GetBool(flagLogClientRetries)
			logClientRequests := v.GetBool(flagLogClientRequests)
			logClientResponses := v.GetBool(flagLogClientResponses)

			partSize := v.GetInt(flagPartSize)

			_ = logger.Log("Configuration", map[string]interface{}{
				"aws_retry_max_attempts":     v.GetInt(flagAWSRetryMaxAttempts),
				"bucket_key_enabled":         bucketKeyEnabled,
				"check_timestamps":           checkTimestamps,
				"max_directory_entries":      maxDirectoryEntries,
				"max_pages":                  maxPages,
				"timestamp_precision":        timestampPrecision.String(),
				"threads":                    threads,
				"exclude":                    syncExclude,
				"limit":                      syncLimit,
				"source_profile":             sourceProfile,
				"source_region":              sourceRegion,
				"source_use_path_style":      sourceUsePathStyle,
				"destination_profile":        destinationProfile,
				"destination_region":         destinationRegion,
				"destination_use_path_style": destinationUsePathStyle,
				"part_size":                  partSize,
			})

			//
			// Synchronizing between s3 buckets
			//
			if strings.HasPrefix(sourceURI, "s3://") &&
				strings.HasPrefix(destinationURI, "s3://") &&
				sourceProfile == destinationProfile &&
				sourceRegion == destinationRegion &&
				sourceEndpoint == destinationEndpoint &&
				sourceUsePathStyle == destinationUsePathStyle &&
				sourceAccessKeyID == destinationAccessKeyID &&
				sourceSecretAccessKey == destinationSecretAccessKey &&
				sourceSessionToken == destinationSessionToken {
				_ = logger.Log("Synchronizing between s3 buckets", map[string]interface{}{
					"source":      sourceURI,
					"destination": destinationURI,
				})
				// set root
				root := "s3://"
				sourceDirectories := s3fs.Split(sourceURI[len("s3://"):])
				sourceBucket := sourceDirectories[0]
				destinationDirectories := s3fs.Split(destinationURI[len("s3://"):])
				destinationBucket := destinationDirectories[0]
				// if same bucket
				if sourceBucket == destinationBucket {
					for i := 0; i < len(sourceDirectories) && i < len(destinationDirectories); i++ {
						if sourceDirectories[i] == destinationDirectories[i] {
							root = filepath.Join(sourceDirectories[0 : i+1]...)
							continue
						}
						break
					}
				}

				// create file system
				if debug {
					fields := map[string]interface{}{
						"root": root,
					}
					if len(sourceEndpoint) > 0 {
						fields["endpoint"] = sourceEndpoint
					}
					_ = logger.Log("Creating shared filesystem", fields)
				}

				fileSystem := InitFileSystem(ctx, &InitFileSystemInput{
					Viper:               v,
					Root:                root,
					Profile:             sourceProfile,
					Partition:           partition,
					DefaultRegion:       sourceRegion,
					MaxDirectoryEntries: maxDirectoryEntries,
					MaxPages:            maxPages,
					BucketKeyEnabled:    bucketKeyEnabled,
					SourceBucket:        sourceBucket,
					DestinationBucket:   destinationBucket,
					// AWS Client
					Endpoint:           sourceEndpoint,
					InsecureSkipVerify: v.GetBool(flagAWSInsecureSkipVerify),
					UsePathStyle:       sourceUsePathStyle,
					RetryMaxAttempts:   v.GetInt(flagAWSRetryMaxAttempts),
					PartSize:           partSize,
					// AWS Credentials
					AccessKeyID:     sourceAccessKeyID,
					SecretAccessKey: sourceSecretAccessKey,
					SessionToken:    sourceSessionToken,
					// Client Mode
					LogClientSigning:   logClientSigning,
					LogClientRetries:   logClientRetries,
					LogClientRequests:  logClientRequests,
					LogClientResponses: logClientResponses,
				})

				sourceRelative := sourceURI[len(root):]
				destinationRelative := destinationURI[len(root):]

				if debug {
					_ = logger.Log("Created shared filesystem", map[string]interface{}{
						"root":        root,
						"source":      sourceRelative,
						"destination": destinationRelative,
					})
				}

				//
				// Synchronize
				//
				count, syncError := fileSystem.Sync(ctx, &fs.SyncInput{
					Delete:             syncDelete,
					Source:             "/" + sourceRelative,
					Destination:        "/" + destinationRelative,
					Exclude:            syncExclude,
					Parents:            syncParents,
					CheckTimestamps:    false, // s3 doesn't allow you to modify last modified
					Limit:              syncLimit,
					Logger:             logger,
					MaxThreads:         threads,
					TimestampPrecision: timestampPrecision,
				})
				if syncError != nil {
					_ = logger.Log("Error synchronizing", map[string]interface{}{
						"source":      sourceURI,
						"destination": destinationURI,
						"err":         syncError.Error(),
					})
					os.Exit(1)
				}

				_ = logger.Log("Done synchronizing", map[string]interface{}{
					"source":      sourceURI,
					"destination": destinationURI,
					"files":       count,
				})

				return nil
			}

			//
			// Synchronizing between local directories
			//
			if (!strings.HasPrefix(sourceURI, "s3://")) && (!strings.HasPrefix(destinationURI, "s3://")) {
				_ = logger.Log("Synchronizing between local directories", map[string]interface{}{
					"source":      sourceURI,
					"destination": destinationURI,
				})

				sourceAbsolutePath, sourceAbsolutePathError := filepath.Abs(sourceURI)
				if sourceAbsolutePathError != nil {
					return fmt.Errorf("error creating absolute path for source: %q", sourceURI)
				}

				destinationAbsolutePath, destinationAbsolutePathError := filepath.Abs(destinationURI)
				if destinationAbsolutePathError != nil {
					return fmt.Errorf("error creating absolute path for destination: %q", destinationURI)
				}

				sourceDirectories := lfs.Split(sourceAbsolutePath)

				destinationDirectories := lfs.Split(destinationAbsolutePath)

				root := ""
				for i := 0; i < len(sourceDirectories) && i < len(destinationDirectories); i++ {
					if sourceDirectories[i] == destinationDirectories[i] {
						root = filepath.Join(sourceDirectories[0 : i+1]...)
						continue
					}
					break
				}

				sourceRelative := filepath.Join(sourceDirectories...)[len(root):]
				destinationRelative := filepath.Join(destinationDirectories...)[len(root):]

				// create file system
				if debug {
					_ = logger.Log("Creating shared filesystem", map[string]interface{}{
						"root": root,
					})
				}

				fileSystem := InitFileSystem(ctx, &InitFileSystemInput{
					Viper:               v,
					Root:                root,
					Profile:             "",
					Partition:           partition,
					DefaultRegion:       "",
					MaxDirectoryEntries: maxDirectoryEntries,
					MaxPages:            maxPages,
					BucketKeyEnabled:    false,
					SourceBucket:        "",
					DestinationBucket:   "",
					// AWS Client
					Endpoint:           "",
					InsecureSkipVerify: v.GetBool(flagAWSInsecureSkipVerify),
					UsePathStyle:       false,
					RetryMaxAttempts:   v.GetInt(flagAWSRetryMaxAttempts),
					PartSize:           -1,
					// AWS Credentials
					AccessKeyID:     "",
					SecretAccessKey: "",
					SessionToken:    "",
					// Client Mode
					LogClientSigning:   false,
					LogClientRetries:   false,
					LogClientRequests:  false,
					LogClientResponses: false,
				})

				if debug {
					_ = logger.Log("Created shared filesystem", map[string]interface{}{
						"root":        root,
						"source":      sourceRelative,
						"destination": destinationRelative,
					})
				}

				//
				// Synchronize
				//
				count, syncError := fileSystem.Sync(ctx, &fs.SyncInput{
					Delete:             syncDelete,
					Source:             sourceRelative,
					Destination:        destinationRelative,
					Exclude:            syncExclude,
					Parents:            syncParents,
					CheckTimestamps:    checkTimestamps,
					Limit:              syncLimit,
					Logger:             logger,
					MaxThreads:         threads,
					TimestampPrecision: timestampPrecision,
				})
				if syncError != nil {
					_ = logger.Log("Error synchronizing", map[string]interface{}{
						"source":      sourceURI,
						"destination": destinationURI,
						"err":         syncError.Error(),
					})
					os.Exit(1)
				}

				_ = logger.Log("Done synchronizing", map[string]interface{}{
					"source":      sourceURI,
					"destination": destinationURI,
					"files":       count,
				})

				return nil
			}

			// create source file system
			if debug {
				fields := map[string]interface{}{
					"root": sourceURI,
				}
				if len(sourceEndpoint) > 0 {
					fields["endpoint"] = sourceEndpoint
				}
				_ = logger.Log("Creating source filesystem", fields)
			}

			sourceBucket := ""
			destinationBucket := ""
			if sourceURI != "s3://" && strings.HasPrefix(sourceURI, "s3://") {
				sourceBucket = s3fs.Split(sourceURI[len("s3://"):])[0]
			}
			if destinationURI != "s3://" && strings.HasPrefix(destinationURI, "s3://") {
				destinationBucket = s3fs.Split(destinationURI[len("s3://"):])[0]
			}

			sourceFileSystem := InitFileSystem(ctx, &InitFileSystemInput{
				Viper:               v,
				Root:                sourceURI,
				Profile:             sourceProfile,
				Partition:           partition,
				DefaultRegion:       sourceRegion,
				MaxDirectoryEntries: maxDirectoryEntries,
				MaxPages:            maxPages,
				BucketKeyEnabled:    false,
				SourceBucket:        sourceBucket,
				DestinationBucket:   "",
				// AWS Client
				Endpoint:           sourceEndpoint,
				InsecureSkipVerify: v.GetBool(flagAWSInsecureSkipVerify),
				UsePathStyle:       sourceUsePathStyle,
				RetryMaxAttempts:   v.GetInt(flagAWSRetryMaxAttempts),
				PartSize:           partSize,
				// AWS Credentials
				AccessKeyID:     sourceAccessKeyID,
				SecretAccessKey: sourceSecretAccessKey,
				SessionToken:    sourceSessionToken,
				// Client Mode
				LogClientSigning:   logClientSigning,
				LogClientRetries:   logClientRetries,
				LogClientRequests:  logClientRequests,
				LogClientResponses: logClientResponses,
			})

			// create destination file system
			if debug {
				fields := map[string]interface{}{
					"root": destinationURI,
				}
				if len(destinationEndpoint) > 0 {
					fields["endpoint"] = destinationEndpoint
				}
				_ = logger.Log("Creating destination filesystem", fields)
			}

			destinationFileSystem := InitFileSystem(ctx, &InitFileSystemInput{
				Viper:               v,
				Root:                destinationURI,
				Profile:             destinationProfile,
				Partition:           partition,
				DefaultRegion:       destinationRegion,
				MaxDirectoryEntries: maxDirectoryEntries,
				MaxPages:            maxPages,
				BucketKeyEnabled:    bucketKeyEnabled,
				SourceBucket:        "",
				DestinationBucket:   destinationBucket,
				// AWS Client
				Endpoint:           destinationEndpoint,
				InsecureSkipVerify: v.GetBool(flagAWSInsecureSkipVerify),
				UsePathStyle:       destinationUsePathStyle,
				RetryMaxAttempts:   v.GetInt(flagAWSRetryMaxAttempts),
				PartSize:           partSize,
				// AWS Credentials
				AccessKeyID:     destinationAccessKeyID,
				SecretAccessKey: destinationSecretAccessKey,
				SessionToken:    destinationSessionToken,
				// Client Mode
				LogClientSigning:   logClientSigning,
				LogClientRetries:   logClientRetries,
				LogClientRequests:  logClientRequests,
				LogClientResponses: logClientResponses,
			})

			//
			// Synchronize
			//
			count, err := fs.Sync(ctx, &fs.SyncInput{
				Delete:                syncDelete,
				Source:                "/",
				SourceFileSystem:      sourceFileSystem,
				Destination:           "/",
				DestinationFileSystem: destinationFileSystem,
				Exclude:               syncExclude,
				Parents:               syncParents,
				CheckTimestamps:       checkTimestamps,
				Limit:                 syncLimit,
				Logger:                logger,
				MaxThreads:            threads,
				TimestampPrecision:    timestampPrecision,
			})
			if err != nil {
				_ = logger.Log("Error synchronizing", map[string]interface{}{
					"source":      sourceURI,
					"destination": destinationURI,
					"err":         err.Error(),
				})
				os.Exit(1)
			}

			_ = logger.Log("Done synchronizing", map[string]interface{}{
				"source":      sourceURI,
				"destination": destinationURI,
				"files":       count,
			})

			return nil

		},
	}
	initSyncCommandFlags(syncCommand.Flags())

	schemesCommand := &cobra.Command{
		Use:                   `schemes`,
		DisableFlagsInUseLine: true,
		Short:                 "show supported schemes",
		SilenceErrors:         true,
		SilenceUsage:          true,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("file")
			fmt.Println("s3")
			return nil
		},
	}

	versionCommand := &cobra.Command{
		Use:                   `version`,
		DisableFlagsInUseLine: true,
		Short:                 "show version",
		SilenceErrors:         true,
		SilenceUsage:          true,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(GoSyncVersion)
			return nil
		},
	}

	rootCommand.AddCommand(layoutsCommand, listCommand, syncCommand, schemesCommand, versionCommand)

	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "gosync: "+err.Error())
		fmt.Fprintln(os.Stderr, "Try \"gosync --help\" for more information.")
		os.Exit(1)
	}
}
