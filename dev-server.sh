#!/bin/bash
# Start SeaweedFS (S3-compatible) and Datasette with datasette-files-s3
set -e

WEED_DIR="/tmp/seaweedfs-dev"
WEED_PID=""
S3_PORT=8333
S3_URL="http://localhost:${S3_PORT}"
BUCKET="datasette-files"
ACCESS_KEY="devkey"
SECRET_KEY="devsecret"

cleanup() {
    echo ""
    echo "Shutting down..."
    if [ -n "$WEED_PID" ]; then
        kill "$WEED_PID" 2>/dev/null || true
        wait "$WEED_PID" 2>/dev/null || true
    fi
    echo "Done."
}
trap cleanup EXIT

mkdir -p "$WEED_DIR"

# Write S3 config with credentials
cat > "$WEED_DIR/s3.json" <<EOFCONFIG
{
  "identities": [
    {
      "name": "dev",
      "credentials": [
        {
          "accessKey": "${ACCESS_KEY}",
          "secretKey": "${SECRET_KEY}"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "Write",
        "List",
        "Tagging"
      ]
    }
  ]
}
EOFCONFIG

echo "Starting SeaweedFS (S3 on port ${S3_PORT}, data in ${WEED_DIR})..."
weed server -dir="$WEED_DIR" -s3 -s3.port="$S3_PORT" -s3.allowEmptyFolder \
    -s3.config="$WEED_DIR/s3.json" -volume.max=10 &
WEED_PID=$!

# Wait for S3 to be ready
echo "Waiting for SeaweedFS S3 endpoint..."
for i in $(seq 1 30); do
    if curl -s -o /dev/null "${S3_URL}" 2>/dev/null; then
        echo "SeaweedFS S3 is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Timed out waiting for SeaweedFS."
        exit 1
    fi
    sleep 1
done

# Create the bucket
echo "Creating bucket '${BUCKET}'..."
AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    aws s3 mb "s3://${BUCKET}" --endpoint-url "$S3_URL" 2>/dev/null || \
    echo "Bucket may already exist, continuing."

echo ""
echo "Starting Datasette..."
echo "  S3 endpoint: ${S3_URL}"
echo "  Bucket: ${BUCKET}"
echo ""

uv run datasette data.db --create --root --secret 1 --reload \
    -s plugins.datasette-files.sources.s3-files.storage s3 \
    -s plugins.datasette-files.sources.s3-files.config.bucket "$BUCKET" \
    -s plugins.datasette-files.sources.s3-files.config.endpoint_url "$S3_URL" \
    -s plugins.datasette-files.sources.s3-files.config.region us-east-1 \
    -s plugins.datasette-files.sources.s3-files.config.access_key_id "$ACCESS_KEY" \
    -s plugins.datasette-files.sources.s3-files.config.secret_access_key "$SECRET_KEY" \
    -s permissions.files-browse true \
    -s permissions.files-upload true \
    -s permissions.files-edit true
