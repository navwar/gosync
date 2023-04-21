# gosync

## Description

**gosync** is a simple program written in [Go](https://golang.org/) for synchronizing two directories specified by URI.  gosync currently supports local directories and AWS S3 buckets as a source or destination.

## Usage

The `gosync` program has 4 sub commands: `completion`, `help`, `sync`, and `version`.  Use `gosync sync` synchronizes the directories.  Use `gosync version` to show the current version.

Below is the usage for the `gosync sync` command.

```text
synchronize source and destination

Usage:
  gosync sync SOURCE DESTINATION

Flags:
      --aws-access-key-id string            AWS Access Key ID
      --aws-assume-role-duration duration   Expiry duration of the STS credentials for assuming a role. (default 15m0s)
      --aws-bucket-key-enabled              bucket key enabled
      --aws-default-region string           AWS Default Region
      --aws-insecure-skip-verify            Skip verification of AWS TLS certificate
      --aws-mfa-serial string               AWS MFA Serial
      --aws-partition string                AWS Partition
      --aws-profile string                  AWS Profile
      --aws-region string                   AWS Region (overrides default region)
      --aws-role-arn string                 AWS Role ARN
      --aws-s3-endpoint string              AWS S3 Endpoint URL
      --aws-s3-use-path-style               Use path-style addressing (default is to use virtual-host-style addressing)
      --aws-secret-access-key string        AWS Secret Access Key
      --aws-session-token string            AWS Session Token
      --check-timestamps                    check timestamps
  -h, --help                                help for sync
      --limit int                           limit number of files copied (default -1)
      --log string                          path to the log output.  Defaults to stdout. (default "-")
      --log-perm string                     file permissions for log output file as unix file mode. (default "0600")
      --max-directory-entries int           maximum directory entries returned (default -1)
      --parents                             create parent directories of destination if they do not exist
      --pool-size int                       pool size (number of concurrent downloads or uploads) (default 1)
      --stop-on-error                       stop copying file if there is an error copying any of them
      --timeout duration                    maximum duration for copying an individual file before aborting
```

## Examples

The below command synchronizes the directory at source to destination.

```shell
aws-vault exec AWS_PROFILE -- gosync sync source destination
```

Below is a command to synchronize two s3 buckets, which uses the [S3 CopyObject API action](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html) when the file size is less than 5GB.  When using the CopyObject action, the file does not have to be downloaded locally to be transferred, so copying is much faster. 

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

**gosync** is written in pure Go, so the only dependency needed to compile the server is [Go](https://golang.org/).  Go can be downloaded from <https://golang.org/dl/>.

This project uses [direnv](https://direnv.net/) to manage environment variables and automatically adding the `bin` and `scripts` folder to the path.  Install direnv and hook it into your shell.  The use of `direnv` is optional as you can always call gosync directly with `bin/gosync`.

If using `macOS`, follow the `macOS` instructions below.

If using `Windows`, following the `Windows` instructions below.

To build a binary for development use `make bin/gosync`.  For a release call `make build_release` or call the `build-release` script directly.  Alternatively, you can always call [gox](https://github.com/mitchellh/gox) or `go build` directly.

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

## Security

Please see [SECURITY.md](SECURITY.md) for more info.

## License

This project constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105.  However, because the project utilizes code licensed from contributors and other third parties, it therefore is licensed under the MIT License.  See LICENSE file for more information.
