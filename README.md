# datasette-files-s3

[![PyPI](https://img.shields.io/pypi/v/datasette-files-s3.svg)](https://pypi.org/project/datasette-files-s3/)
[![Changelog](https://img.shields.io/github/v/release/datasette/datasette-files-s3?include_prereleases&label=changelog)](https://github.com/datasette/datasette-files-s3/releases)
[![Tests](https://github.com/datasette/datasette-files-s3/actions/workflows/test.yml/badge.svg)](https://github.com/datasette/datasette-files-s3/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/datasette/datasette-files-s3/blob/main/LICENSE)

S3 storage backend for [datasette-files](https://github.com/datasette/datasette-files).

## Installation

Install this plugin in the same environment as Datasette.
```bash
datasette install datasette-files-s3
```

## Usage

Configure a datasette-files source to use S3 storage by setting `"storage": "s3"` and providing the required configuration options:

```yaml
plugins:
  datasette-files:
    sources:
      my-s3-files:
        storage: s3
        config:
          bucket: my-bucket-name
          region: us-east-1
          access_key_id: AKIAIOSFODNN7EXAMPLE
          secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Or using Datasette's `-s` flag:

```bash
datasette data.db \
    -s plugins.datasette-files.sources.my-s3-files.storage s3 \
    -s plugins.datasette-files.sources.my-s3-files.config.bucket my-bucket-name \
    -s plugins.datasette-files.sources.my-s3-files.config.region us-east-1
```

### Configuration options

- **bucket** (required): The name of the S3 bucket.
- **region** (optional, default `us-east-1`): The AWS region.
- **prefix** (optional): A prefix to add to all S3 object keys. This allows you to store files under a specific path within the bucket. A trailing slash will be added automatically if not provided - `"uploads"` and `"uploads/"` are equivalent.
- **endpoint_url** (optional): A custom S3 endpoint URL, for use with S3-compatible services.
- **access_key_id** (optional): AWS access key ID.
- **secret_access_key** (optional): AWS secret access key.

### Authentication

The plugin resolves AWS credentials using the following priority:

1. **Direct configuration**: `access_key_id` and `secret_access_key` in the config block.
2. **datasette-secrets**: If [datasette-secrets](https://github.com/datasette/datasette-secrets) is installed, the plugin will look for `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` secrets.
3. **Default AWS credential chain**: If no credentials are provided through the above methods, the plugin falls back to the default AWS credential chain (environment variables, IAM roles, etc.).

### Prefix

The `prefix` option lets you scope all files to a specific path within the bucket. For example, with `prefix: "uploads/"`, a file uploaded as `photo.jpg` will be stored at the S3 key `uploads/photo.jpg`.

It does not matter whether you include a trailing slash or not - `"uploads"` and `"uploads/"` will both result in files stored under `uploads/`.

## Development

To set up this plugin locally, first checkout the code.
```bash
cd datasette-files-s3
```

Run tests like this:
```bash
uv run pytest
```

You can use [SeaweedFS](https://github.com/seaweedfs/seaweedfs) to run a local development server against a local imitation of the S3 API:
```bash
brew install seaweedfs
./dev-server.sh
```
To run a local development server against a real S3 bucket, create a `dev-s3.sh` script (this file is in `.gitignore`):

```bash
#!/bin/bash
set -e

BUCKET="your-bucket-name"
REGION="us-east-1"
ACCESS_KEY="your-access-key-id"
SECRET_KEY="your-secret-access-key"

uv run datasette data.db --create --internal internal.db --root --secret 1 --reload \
    -s plugins.datasette-files.sources.s3-live.storage s3 \
    -s plugins.datasette-files.sources.s3-live.config.bucket "$BUCKET" \
    -s plugins.datasette-files.sources.s3-live.config.region "$REGION" \
    -s plugins.datasette-files.sources.s3-live.config.access_key_id "$ACCESS_KEY" \
    -s plugins.datasette-files.sources.s3-live.config.secret_access_key "$SECRET_KEY" \
    -s plugins.datasette-files.sources.s3-live.config.prefix "demo-prefix/" \
    -s permissions.files-browse true \
    -s permissions.files-upload true \
    -s permissions.files-edit true
```

Then run it with `bash dev-s3.sh` and follow the login token URL printed to the console.
