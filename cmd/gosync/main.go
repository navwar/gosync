// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/deptofdefense/gosync/pkg/fs"
	"github.com/deptofdefense/gosync/pkg/lfs"
	"github.com/deptofdefense/gosync/pkg/log"
	"github.com/deptofdefense/gosync/pkg/s3fs"
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
	// TLS
	flagAWSInsecureSkipVerify = "aws-insecure-skip-verify"
	// Miscellaneous
	flagAWSS3Endpoint     = "aws-s3-endpoint"
	flagAWSS3UsePathStyle = "aws-s3-use-path-style"
	flagBucketKeyEnabled  = "aws-bucket-key-enabled"
	flagAWSS3MaxPages     = "aws-s3-max-pages"
)

// AWS Defaults
const (
	// Role
	DefaultAWSAssumeRoleDuration = 15 * time.Minute
	MinimumAWSAssumeRoleDuration = 15 * time.Minute
)

// Sync Flags
const (
	flagParents             = "parents"
	flagSyncLimit           = "limit"
	flagPoolSize            = "pool-size"
	flagStopOnError         = "stop-on-error"
	flagTimeout             = "timeout"
	flagMaxDirectoryEntries = "max-directory-entries"
	//
	flagCheckTimestamps = "check-timestamps"
)

// Sync Defaults
const (
	DefaultPoolSize = 1
	DefaultLimit    = -1
)

// Log Flags
const (
	flagLogPath = "log"
	flagLogPerm = "log-perm"
)

// InitAWSFlags initializes the AWS flags.
func initAWSFlags(flag *pflag.FlagSet) {
	// Profile
	flag.String(flagAWSPartition, "", "AWS Partition")
	flag.String(flagAWSProfile, "", "AWS Profile")
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
	// TLS
	flag.Bool(flagAWSInsecureSkipVerify, false, "Skip verification of AWS TLS certificate")
	// Misceallenous
	flag.String(flagAWSS3Endpoint, "", "AWS S3 Endpoint URL")
	flag.Bool(flagAWSS3UsePathStyle, false, "Use path-style addressing (default is to use virtual-host-style addressing)")
	flag.Bool(flagBucketKeyEnabled, false, "bucket key enabled")
	flag.Int(flagAWSS3MaxPages, -1, "maximum number of pages to return from AWS S3")
}

func initSyncFlags(flag *pflag.FlagSet) {
	flag.Bool(flagParents, false, "create parent directories of destination if they do not exist")
	flag.Int(flagSyncLimit, DefaultLimit, "limit number of files copied")
	flag.Int(flagPoolSize, DefaultPoolSize, "pool size (number of concurrent downloads or uploads)")
	flag.Bool(flagStopOnError, false, "stop copying file if there is an error copying any of them")
	flag.Duration(flagTimeout, 0*time.Second, "maximum duration for copying an individual file before aborting")
	flag.Bool(flagCheckTimestamps, false, "check timestamps")
	flag.Int(flagMaxDirectoryEntries, -1, "maximum directory entries returned")
}

func initLogFlags(flag *pflag.FlagSet) {
	flag.String(flagLogPath, "-", "path to the log output.  Defaults to stdout.")
	flag.String(flagLogPerm, "0600", "file permissions for log output file as unix file mode.")
}

func initFlags(flag *pflag.FlagSet) {
	initAWSFlags(flag)
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

func checkConfig(v *viper.Viper, args []string) error {
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
	return nil
}

func initS3Client(v *viper.Viper, partition string, region string) *s3.Client {
	accessKeyID := v.GetString(flagAWSAccessKeyID)
	secretAccessKey := v.GetString(flagAWSSecretAccessKey)
	sessionToken := v.GetString(flagAWSSessionToken)
	usePathStyle := v.GetBool(flagAWSS3UsePathStyle)

	config := aws.Config{
		RetryMaxAttempts: 3,
		Region:           region,
	}

	if e := v.GetString(flagAWSS3Endpoint); len(e) > 0 {
		config.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(func(service string, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				endpoint := aws.Endpoint{
					PartitionID:   partition,
					URL:           e,
					SigningRegion: region,
				}
				return endpoint, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
	}

	if len(accessKeyID) > 0 && len(secretAccessKey) > 0 {
		config.Credentials = credentials.NewStaticCredentialsProvider(
			accessKeyID,
			secretAccessKey,
			sessionToken)
	}

	insecureSkipVerify := v.GetBool(flagAWSInsecureSkipVerify)
	if insecureSkipVerify {
		config.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	client := s3.NewFromConfig(config, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
	})

	return client
}

func initFileSystem(ctx context.Context, v *viper.Viper, rootPath string, partition string, defaultRegion string, maxDirectoryEntries int, maxPages int, bucketKeyEnabled bool) fs.FileSystem {
	if strings.HasPrefix(rootPath, "s3://") {
		//
		// List all buckets in accounts and store creation date
		//

		clients := map[string]*s3.Client{}

		//
		// Initialize Default Client
		//

		clients[defaultRegion] = initS3Client(v, partition, defaultRegion)

		//

		bucketCreationDates := map[string]time.Time{}
		bucketRegions := map[string]string{}

		//
		// List Buckets
		//

		listBucketsOutput, err := clients[defaultRegion].ListBuckets(ctx, &s3.ListBucketsInput{})
		if err == nil {
			for _, b := range listBucketsOutput.Buckets {
				bucketCreationDates[aws.ToString(b.Name)] = aws.ToTime(b.CreationDate)
			}
		}

		//
		// If root is the account
		//

		if rootPath == "s3://" {
			//
			// Get Region for each Bucket
			//
			for bucketName, _ := range bucketCreationDates {
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
			}

			//
			// Get Client for each region with a bucket
			//

			for _, bucketRegion := range bucketRegions {
				if _, ok := clients[bucketRegion]; !ok {
					clients[bucketRegion] = initS3Client(v, partition, bucketRegion)
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
				bucketKeyEnabled)
		}

		//
		// If root is a bucket
		//

		rootParts := strings.Split(rootPath[len("s3://"):], "/")
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
			clients[bucketRegion] = initS3Client(v, partition, bucketRegion)
		} else {
			// It GetBucketLocation is not allowed, assume that the default region is the region containing the bucket
			clients[defaultRegion] = initS3Client(v, partition, defaultRegion)
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
			bucketKeyEnabled)
	}

	if strings.HasPrefix(rootPath, "file://") {
		return lfs.NewLocalFileSystem(rootPath[len("file://"):])
	}

	return lfs.NewLocalFileSystem(rootPath)
}

func initLogger(path string, perm string) (*log.SimpleLogger, error) {

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

func main() {
	rootCommand := &cobra.Command{
		Use:                   `gosync [flags]`,
		DisableFlagsInUseLine: true,
		Short:                 `gosync is a simple command line program for synchronizing two directories specified by URI.  gosync currently supports local directories and AWS S3 buckets as a source or destination.  AWS S3 buckets are specified using the "s3://" scheme.  Local files are specified using the "file://" scheme or a path without a scheme.  gosync synchronizes regular files and will create directories as needed if the parents flag is set.`,
	}

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

			if errConfig := checkConfig(v, args); errConfig != nil {
				return errConfig
			}

			logger, err := initLogger(v.GetString(flagLogPath), v.GetString(flagLogPerm))
			if err != nil {
				return fmt.Errorf("error initializing logger: %w", err)
			}

			sourceURI := args[0]
			destinationURI := args[1]

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

			maxDirectoryEntries := v.GetInt(flagMaxDirectoryEntries)
			maxPages := v.GetInt(flagAWSS3MaxPages)

			syncLimit := v.GetInt(flagSyncLimit)

			bucketKeyEnabled := v.GetBool(flagBucketKeyEnabled)

			checkTimestamps := v.GetBool(flagCheckTimestamps)

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
				destinationDirectories := s3fs.Split(destinationURI[len("s3://"):])
				// if same bucket
				if sourceDirectories[0] == destinationDirectories[0] {
					for i := 0; i < len(sourceDirectories) && i < len(destinationDirectories); i++ {
						if sourceDirectories[i] == destinationDirectories[i] {
							root = filepath.Join(sourceDirectories[0 : i+1]...)
							continue
						}
						break
					}
				}

				// create file system
				_ = logger.Log("Creating filesystem", map[string]interface{}{
					"root": root,
				})

				fileSystem := initFileSystem(ctx, v, root, partition, region, maxDirectoryEntries, maxPages, bucketKeyEnabled)
				sourceRelative := sourceURI[len(root):]
				destinationRelative := destinationURI[len(root):]
				_ = logger.Log("Relative paths", map[string]interface{}{
					"source":      sourceRelative,
					"destination": destinationRelative,
				})

				/* Check source and destination
				sourceDirectoryEntries, err := fileSystem.ReadDir(ctx, sourceRelative)
				if err != nil {
					return fmt.Errorf("error checking source: %w", err)
				}
				_ = logger.Log("Files at source", map[string]interface{}{
					"files": sourceDirectoryEntries,
				})
				destinationDirectoryEntries, err := fileSystem.ReadDir(ctx, destinationRelative)
				if err != nil {
					return fmt.Errorf("error checking destination: %w", err)
				}
				_ = logger.Log("Files at destination", map[string]interface{}{
					"files": destinationDirectoryEntries,
				})
				*/

				//
				// Synchronize
				//
				count, err := fileSystem.Sync(ctx, sourceRelative, destinationRelative, true, false, syncLimit, logger)
				if err != nil {
					return fmt.Errorf("error synchronizing from %q to %q: %w", sourceURI, destinationURI, err)
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

				sourceAbsolutePath, err := filepath.Abs(sourceURI)
				if err != nil {
					return fmt.Errorf("error creating absolute path for source: %q", sourceURI)
				}

				destinationAbsolutePath, err := filepath.Abs(destinationURI)
				if err != nil {
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
				_ = logger.Log("Creating filesystem", map[string]interface{}{
					"root": root,
				})

				fileSystem := initFileSystem(ctx, v, root, partition, region, maxDirectoryEntries, maxPages, bucketKeyEnabled)
				_ = logger.Log("Relative paths", map[string]interface{}{
					"source":      sourceRelative,
					"destination": destinationRelative,
				})

				/* Check source and destination
				sourceDirectoryEntries, err := fileSystem.ReadDir(ctx, sourceRelative)
				if err != nil {
					return fmt.Errorf("error checking source: %w", err)
				}
				_ = logger.Log("Files at source", map[string]interface{}{
					"files": sourceDirectoryEntries,
				})
				destinationDirectoryEntries, err := fileSystem.ReadDir(ctx, destinationRelative)
				if err != nil {
					if ! fileSystem.IsNotExist(err) {
						return fmt.Errorf("error checking destination: %w", err)
					}
					destinationDirectoryEntries = nil
				}

				_ = logger.Log("Files at destination", map[string]interface{}{
					"files": destinationDirectoryEntries,
				})
				*/

				//
				// Synchronize
				//
				count, err := fileSystem.Sync(ctx, sourceRelative, destinationRelative, true, checkTimestamps, syncLimit, logger)
				if err != nil {
					return fmt.Errorf("error synchronizing from %q to %q: %w", sourceURI, destinationURI, err)
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
			sourceFileSystem := initFileSystem(ctx, v, sourceURI, partition, region, maxDirectoryEntries, maxPages, bucketKeyEnabled)

			sourceDirectoryEntries, err := sourceFileSystem.ReadDir(ctx, "/")
			_ = logger.Log("Files at source", map[string]interface{}{
				"files": sourceDirectoryEntries,
			})

			// Destination File System
			_ = logger.Log("Creating destination filesystem", map[string]interface{}{
				"uri": destinationURI,
			})

			destinationFileSystem := initFileSystem(ctx, v, destinationURI, partition, region, maxDirectoryEntries, maxPages, bucketKeyEnabled)

			destinationDirectoryEntries, err := destinationFileSystem.ReadDir(ctx, "/")
			_ = logger.Log("Files at destination", map[string]interface{}{
				"files": destinationDirectoryEntries,
			})

			//
			// Synchronize
			//
			count, err := fs.Sync(
				ctx,
				"/",
				sourceFileSystem,
				"/",
				destinationFileSystem,
				true,
				checkTimestamps,
				syncLimit,
				logger)
			if err != nil {
				return fmt.Errorf("error synchronizing from %q to %q: %w", sourceURI, destinationURI, err)
			}

			_ = logger.Log("Done synchronizing", map[string]interface{}{
				"source":      sourceURI,
				"destination": destinationURI,
				"files":       count,
			})

			return nil

		},
	}
	initFlags(syncCommand.Flags())

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

	rootCommand.AddCommand(syncCommand, versionCommand)

	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "gosync: "+err.Error())
		fmt.Fprintln(os.Stderr, "Try \"gosync --help\" for more information.")
		os.Exit(1)
	}
}
