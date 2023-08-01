# gosync

## Description

**gosync** is a simple program written in [Go](https://go.dev/) for synchronizing two directories specified by URI.  gosync currently supports local directories and AWS S3 buckets as a source or destination.

## Usage

The `gosync` program has 5 sub commands: `completion`, `help`, `layouts`, `list`, `schemes`, `sync`, and `version`.  Use `gosync sync` to synchronize a source and destination.  Use `gosync version` to show the current version.

Flag values can be set using command line arguments (e.g., `--aws-mfa-serial=XYZ`) or environment variables (`AWS_MFA_SERIAL=XYZ`).  For integer flags, an underscore character `_` may appear between successive digits to enhance readability.

Below is the usage for the `gosync sync` command.

```text
synchronize source and destination

Usage:
  gosync sync SOURCE DESTINATION

Flags:
  -a, --all                                 Include directory entries whose names begin with a dot (‘.’).
      --aws-access-key-id string            AWS Access Key ID
      --aws-assume-role-duration duration   Expiry duration of the STS credentials for assuming a role. (default 15m0s)
      --aws-bucket-key-enabled              bucket key enabled
      --aws-default-region string           AWS Default Region
      --aws-insecure-skip-verify            Skip verification of AWS TLS certificate
      --aws-mfa-serial string               AWS MFA Serial
      --aws-partition string                AWS Partition (default "aws")
      --aws-profile string                  AWS Profile (default "default")
      --aws-region string                   AWS Region (overrides default region)
      --aws-retry-max-attempts int          the maximum number attempts an AWS API client will call an operation that fails with a retryable error. (default 5)
      --aws-role-arn string                 AWS Role ARN
      --aws-s3-endpoint string              AWS S3 Endpoint URL
      --aws-s3-use-path-style               Use path-style addressing (default is to use virtual-host-style addressing)
      --aws-secret-access-key string        AWS Secret Access Key
      --aws-session-token string            AWS Session Token
      --check-timestamps                    check timestamps are equal
  -d, --debug                               print debug messages
      --delete                              delete files at destination that do not exist at source
  -e, --exclude string                      a colon-separated list of paths to exclude with support for wildcards, e.g, path, *path, path*, or *path*.
  -h, --help                                help for sync
      --human-readable-file-size            display file sizes in human-readable format
      --limit int                           limit number of files copied (default -1)
      --log-client-requests                 log AWS client requests
      --log-client-responses                log AWS client responses
      --log-client-retries                  log AWS client retries
      --log-client-signing                  log AWS client signature requests
  -f, --log-format string                   output log format.  Either jsonl or text. (default "text")
      --log-path string                     path to the log output.  Defaults to the operating system's stdout device. (default "-")
      --log-perm string                     file permissions for log output file as unix file mode. (default "0600")
      --magic-number                        include magic number in output as hex
      --max-directory-entries int           maximum directory entries for each page returned by the filesystem (default -1)
      --max-pages int                       maximum number of pages to return from the filesystem when reading a directory (default -1)
  -p, --parents                             create parent directories for destination if they do not exist
      --part-size int                       size of parts in bytes when transferring to S3 (minimum 4194304) (default 104857600)
  -r, --recursive                           recursively list sub-directories breadth-first
      --threads int                         maximum number of parallel threads (default 1)
  -t, --time-layout string                  the layout to use for file timestamps.  Use go layout format, or the name of a layout.  Use gosync layouts to show all named layouts. (default "Default")
  -z, --time-zone string                    the timezone to use for file timestamps (default "Local")
      --timestamp-precision duration        precision to use when checking timestamps (default 1s)
```

## Examples

The below command synchronizes the directory at source to destination.

```shell
aws-vault exec AWS_PROFILE -- gosync sync source destination
```

Below is a command to synchronize two S3 buckets in the same partition, which doesn't required downloading the data locally.

```shell
aws-vault exec AWS_PROFILE -- gosync sync s3://SOURE_BUCKET s3://DESTINATION_BUCKET
```

Below is a command to download data from a source AWS S3 bucket to a local destination directory.

```shell
aws-vault exec AWS_PROFILE -- gosync sync s3://SOURE_BUCKET local_destination
```

Below is a command to download data from a local source directory to a destination AWS S3 Bucket.

```shell
aws-vault exec AWS_PROFILE -- gosync sync local_source s3://DESTINATION_BUCKET 
```

## Building

**gosync** is written in pure Go, so the only dependency needed to compile the server is [Go](https://go.dev/).  Go can be downloaded from <https://go.dev/dl/>.

This project uses [direnv](https://direnv.net/) to manage environment variables and automatically adding the `bin` and `scripts` folder to the path.  Install direnv and hook it into your shell.  The use of `direnv` is optional as you can always call gosync directly with `bin/gosync`.

If using `macOS`, follow the `macOS` instructions below.

If using `Windows`, following the `Windows` instructions below.

To rebuild a binary for development use `make rebuild`.  For a release call `make build_release` or call the `build-release` script directly.  Alternatively, you can call [gox](https://github.com/mitchellh/gox) or `go build` directly.

### macOS

You can install `go` on macOS using homebrew with `brew install go`.

To install `direnv` on `macOS` use `brew install direnv`.  If using bash, then add `eval \"$(direnv hook bash)\"` to the `~/.bash_profile` file .  If using zsh, then add `eval \"$(direnv hook zsh)\"` to the `~/.zshrc` file.

### Windows

Download the latest Windows release for `go` from [https://go.dev/dl/](https://go.dev/dl/) and install it.

For a `PowerShell` terminal, call the `.\env.ps1` file to update the local environment variables.

## Testing

**CLI**

To run CLI testes use `make test_cli`, which uses [shUnit2](https://github.com/kward/shunit2).  If you recive a `shunit2:FATAL Please declare TMPDIR with path on partition with exec permission.` error, you can modify the `TMPDIR` environment variable in line or with `export TMPDIR=<YOUR TEMP DIRECTORY HERE>`. For example:

```shell
TMPDIR="/usr/local/tmp" make test_cli
```

**Go**

To run Go tests use `make test_go` (or `bash scripts/test.sh`), which runs unit tests, `go vet`, `go vet with shadow`, [errcheck](https://github.com/kisielk/errcheck), [staticcheck](https://staticcheck.io/), and [misspell](https://github.com/client9/misspell).

## Contributing

We'd love to have your contributions!  Please see [CONTRIBUTING.md](CONTRIBUTING.md) for more info.

## License

This project constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105.  However, because the project utilizes code licensed from contributors and other third parties, it therefore is licensed under the MIT License.  See LICENSE file for more information.
