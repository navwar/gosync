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
	if args[0] == args[1] {
		return fmt.Errorf("source and destination must be different: %q", args[0])
	}
	if strings.HasPrefix(args[0], args[1]) {
		return fmt.Errorf("destination %q cannot be a prefix for source %q", args[1], args[0])
	}
	if strings.HasPrefix(args[1], args[0]) {
		return fmt.Errorf("source %q cannot be a prefix for destination %q", args[0], args[1])
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
	return nil
}

func initS3Client(ctx context.Context, v *viper.Viper, profile string, partition string, region string) *s3.Client {
	accessKeyID := v.GetString(flagAWSAccessKeyID)
	secretAccessKey := v.GetString(flagAWSSecretAccessKey)
	sessionToken := v.GetString(flagAWSSessionToken)
	usePathStyle := v.GetBool(flagAWSS3UsePathStyle)
	retryMaxAttempts := v.GetInt(flagAWSRetryMaxAttempts)

	clientLogMode := aws.ClientLogMode(0)
	if logClientSigning := v.GetBool(flagLogClientSigning); logClientSigning {
		clientLogMode |= aws.LogSigning
	}
	if logClientRetries := v.GetBool(flagLogClientRetries); logClientRetries {
		clientLogMode |= aws.LogRetries
	}
	if logClientRequests := v.GetBool(flagLogClientRequests); logClientRequests {
		clientLogMode |= aws.LogRequest
	}
	if logClientResponses := v.GetBool(flagLogClientResponses); logClientResponses {
		clientLogMode |= aws.LogResponse
	}
	c := aws.Config{
		ClientLogMode:    clientLogMode,
		RetryMaxAttempts: retryMaxAttempts,
		Region:           region,
		Logger:           log.NewClientLogger(os.Stdout),
	}

	if len(accessKeyID) > 0 && len(secretAccessKey) > 0 {
		c.Credentials = credentials.NewStaticCredentialsProvider(
			accessKeyID,
			secretAccessKey,
			sessionToken)
	} else {
		sharedConfig, err := config.LoadSharedConfigProfile(ctx, profile)
		if err == nil {
			c.Credentials = credentials.NewStaticCredentialsProvider(
				sharedConfig.Credentials.AccessKeyID,
				sharedConfig.Credentials.SecretAccessKey,
				"")
		}
	}

	insecureSkipVerify := v.GetBool(flagAWSInsecureSkipVerify)
	if insecureSkipVerify {
		c.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	client := s3.NewFromConfig(c, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
		if e := v.GetString(flagAWSS3Endpoint); len(e) > 0 {
			o.BaseEndpoint = aws.String(e)
		}
	})

	return client
}

func initFileSystem(ctx context.Context, v *viper.Viper, root string, profile string, partition string, defaultRegion string, maxDirectoryEntries int, maxPages int, bucketKeyEnabled bool, sourceBucket string, destinationBucket string) fs.FileSystem {

	if strings.HasPrefix(root, "file://") {
		return lfs.NewLocalFileSystem(root[len("file://"):])
	}

	if strings.HasPrefix(root, "s3://") {
		//
		// List all buckets in accounts and store creation date
		//

		clients := map[string]*s3.Client{}

		//
		// Initialize Default Client
		//

		clients[defaultRegion] = initS3Client(ctx, v, profile, partition, defaultRegion)

		//

		bucketCreationDates := map[string]time.Time{}
		bucketRegions := map[string]string{}

		//
		// List Buckets
		//

		listBucketsOutput, err := clients[defaultRegion].ListBuckets(ctx, &s3.ListBucketsInput{})
		if err == nil {
			// if root is s3:// and don't know enough to filter by source or destination
			if root == "s3://" && sourceBucket == "" && destinationBucket == "" {
				// do not filter
				for _, b := range listBucketsOutput.Buckets {
					bucketCreationDates[aws.ToString(b.Name)] = aws.ToTime(b.CreationDate)
				}
			} else {
				for _, b := range listBucketsOutput.Buckets {
					if bucketName := aws.ToString(b.Name); bucketName == sourceBucket || bucketName == destinationBucket {
						bucketCreationDates[bucketName] = aws.ToTime(b.CreationDate)
					}
				}
			}
		}

		//
		// If root is the account
		//

		if root == "s3://" {
			//
			// Get Region for each Bucket
			//
			for bucketName := range bucketCreationDates {
				getBucketLocationOutput, getBucketLocationError := clients[defaultRegion].GetBucketLocation(ctx, &s3.GetBucketLocationInput{
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
					clients[bucketRegion] = initS3Client(ctx, v, profile, partition, bucketRegion)
				}
			}

			return s3fs.NewS3FileSystem(
				defaultRegion,
				"",
				"",
				clients,
				bucketRegions,
				bucketCreationDates,
				maxDirectoryEntries,
				maxPages,
				bucketKeyEnabled,
				v.GetInt(flagPartSize))
		}

		//
		// If root is a bucket
		//

		rootParts := strings.Split(root[len("s3://"):], "/")
		bucketName := rootParts[0]
		prefix := strings.Join(rootParts[1:], "/")

		getBucketLocationOutput, err := clients[defaultRegion].GetBucketLocation(ctx, &s3.GetBucketLocationInput{
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
			clients[bucketRegion] = initS3Client(ctx, v, profile, partition, bucketRegion)
		} else {
			// It GetBucketLocation is not allowed, assume that the default region is the region containing the bucket
			clients[defaultRegion] = initS3Client(ctx, v, profile, partition, defaultRegion)
		}

		return s3fs.NewS3FileSystem(
			defaultRegion,
			bucketName,
			prefix,
			clients,
			bucketRegions,
			bucketCreationDates,
			maxDirectoryEntries,
			maxPages,
			bucketKeyEnabled,
			v.GetInt(flagPartSize))
	}

	return lfs.NewLocalFileSystem(root)
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
			_ = logger.Log("Creating filesystem", map[string]interface{}{
				"root": uri,
			})

			fs := initFileSystem(
				ctx,
				v,
				uri,
				profile,
				partition,
				region,
				maxDirectoryEntries,
				maxPages,
				false,
				bucket,
				"",
			)

			//
			// List
			//

			recursive := v.GetBool(flagRecursive)

			directoryEntries, err := fs.ReadDir(ctx, "/", recursive)
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
				mn, err := fs.MagicNumbers(ctx, names, -1)
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

			logger, err := initLogger(v.GetString(flagLogPath), v.GetString(flagLogPerm))
			if err != nil {
				return fmt.Errorf("error initializing logger: %w", err)
			}

			sourceURI := args[0]
			destinationURI := args[1]

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

			_ = logger.Log("Configuration", map[string]interface{}{
				"aws_retry_max_attempts": v.GetInt(flagAWSRetryMaxAttempts),
				"bucket_key_enabled":     bucketKeyEnabled,
				"check_timestamps":       checkTimestamps,
				"max_directory_entries":  maxDirectoryEntries,
				"max_pages":              maxPages,
				"timestamp_precision":    timestampPrecision.String(),
				"threads":                threads,
				"exclude":                syncExclude,
				"limit":                  syncLimit,
			})

			//
			// Synchronizing between s3 buckets
			//
			if strings.HasPrefix(sourceURI, "s3://") && strings.HasPrefix(destinationURI, "s3://") {
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
				_ = logger.Log("Creating shared filesystem", map[string]interface{}{
					"root": root,
				})

				fileSystem := initFileSystem(
					ctx,
					v,
					root,
					profile,
					partition,
					region,
					maxDirectoryEntries,
					maxPages,
					bucketKeyEnabled,
					sourceBucket,
					destinationBucket,
				)
				sourceRelative := sourceURI[len(root):]
				destinationRelative := destinationURI[len(root):]

				_ = logger.Log("Created shared filesystem", map[string]interface{}{
					"root":        root,
					"source":      sourceRelative,
					"destination": destinationRelative,
				})

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

				// Create file system
				_ = logger.Log("Creating shared filesystem", map[string]interface{}{
					"root": root,
				})

				fileSystem := initFileSystem(
					ctx,
					v,
					root,
					profile,
					partition,
					region,
					maxDirectoryEntries,
					maxPages,
					bucketKeyEnabled,
					"",
					"",
				)

				_ = logger.Log("Created shared filesystem", map[string]interface{}{
					"root":        root,
					"source":      sourceRelative,
					"destination": destinationRelative,
				})

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

			// Source File System
			_ = logger.Log("Creating source filesystem", map[string]interface{}{
				"uri": sourceURI,
			})
			sourceBucket := ""
			destinationBucket := ""
			if sourceURI != "s3://" && strings.HasPrefix(sourceURI, "s3://") {
				sourceBucket = s3fs.Split(sourceURI[len("s3://"):])[0]
			}
			if destinationURI != "s3://" && strings.HasPrefix(destinationURI, "s3://") {
				destinationBucket = s3fs.Split(destinationURI[len("s3://"):])[0]
			}
			sourceFileSystem := initFileSystem(
				ctx,
				v,
				sourceURI,
				profile,
				partition,
				region,
				maxDirectoryEntries,
				maxPages,
				bucketKeyEnabled,
				sourceBucket,
				"",
			)

			// Destination File System
			_ = logger.Log("Creating destination filesystem", map[string]interface{}{
				"uri": destinationURI,
			})

			destinationFileSystem := initFileSystem(
				ctx,
				v,
				destinationURI,
				profile,
				partition,
				region,
				maxDirectoryEntries,
				maxPages,
				bucketKeyEnabled,
				"",
				destinationBucket,
			)

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
