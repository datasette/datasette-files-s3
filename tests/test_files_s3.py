import hashlib
import json
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError

import aioboto3
import pytest
from aiomoto import mock_aws
from datasette.app import Datasette

from datasette_files_s3 import S3Storage


async def _bytes_stream(data: bytes):
    yield data


class FakeCredentialsResponse:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


BUCKET = "test-bucket"
REGION = "us-east-1"


@pytest.fixture
def s3_mock():
    with mock_aws():
        yield


async def _create_bucket():
    session = aioboto3.Session()
    async with session.client("s3", region_name=REGION) as client:
        await client.create_bucket(Bucket=BUCKET)


async def _make_storage(prefix="", bucket=BUCKET):
    storage = S3Storage()
    await storage.configure(
        {"bucket": bucket, "prefix": prefix, "region": REGION},
    )
    return storage


def _credentials_payload(
    *,
    access_key_id="temporary-key-id",
    secret_access_key="temporary-secret",
    session_token="temporary-session-token",
    expiration=None,
    folder=None,
):
    expiration = expiration or (
        datetime.now(timezone.utc) + timedelta(hours=1)
    ).isoformat().replace("+00:00", "Z")
    folder = folder or f"s3://{BUCKET}/team-1/"
    return {
        "AccessKeyId": access_key_id,
        "SecretAccessKey": secret_access_key,
        "SessionToken": session_token,
        "Expiration": expiration,
        "S3Folder": folder,
    }


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-files-s3" in installed_plugins


@pytest.mark.asyncio
async def test_configure(s3_mock):
    storage = S3Storage()
    await storage.configure(
        {"bucket": "my-bucket", "prefix": "uploads/", "region": "eu-west-1"},
    )
    assert storage.bucket == "my-bucket"
    assert storage.prefix == "uploads/"
    assert storage.region == "eu-west-1"


@pytest.mark.asyncio
async def test_configure_defaults(s3_mock):
    storage = S3Storage()
    await storage.configure({"bucket": "my-bucket"})
    assert storage.prefix == ""
    assert storage.region == "us-east-1"


@pytest.mark.asyncio
async def test_configure_with_credentials_url(monkeypatch, s3_mock):
    seen = {}
    payload = _credentials_payload(folder="s3://dynamic-bucket/team-1/")

    def fake_urlopen(request):
        seen["url"] = request.full_url
        seen["method"] = request.get_method()
        seen["content_type"] = request.get_header("Content-type")
        seen["body"] = request.data
        return FakeCredentialsResponse(payload)

    monkeypatch.setattr("datasette_files_s3.urlopen", fake_urlopen)

    storage = S3Storage()
    await storage.configure(
        {
            "credentials_url": "https://example.com/credentials",
            "credentials_url_secret": "secret value",
        },
    )

    assert seen["url"] == "https://example.com/credentials"
    assert seen["method"] == "POST"
    assert seen["content_type"] == "application/x-www-form-urlencoded"
    assert seen["body"] == b"secret=secret+value"
    assert storage.access_key_id == payload["AccessKeyId"]
    assert storage.secret_access_key == payload["SecretAccessKey"]
    assert storage.session_token == payload["SessionToken"]
    assert storage.credentials_expiration == datetime.fromisoformat(
        payload["Expiration"].replace("Z", "+00:00")
    )
    assert storage.s3_folder == payload["S3Folder"]
    assert storage.bucket == "dynamic-bucket"
    assert storage.prefix == "team-1/"


@pytest.mark.asyncio
async def test_credentials_url_refreshes_after_expiration(monkeypatch, s3_mock):
    await _create_bucket()
    payloads = [
        _credentials_payload(
            access_key_id="first-key",
            session_token="first-token",
        ),
        _credentials_payload(
            access_key_id="second-key",
            session_token="second-token",
            expiration=(datetime.now(timezone.utc) + timedelta(hours=2))
            .isoformat()
            .replace("+00:00", "Z"),
        ),
    ]
    seen = {"count": 0}

    def fake_urlopen(request):
        payload = payloads[seen["count"]]
        seen["count"] += 1
        return FakeCredentialsResponse(payload)

    monkeypatch.setattr("datasette_files_s3.urlopen", fake_urlopen)

    storage = S3Storage()
    await storage.configure(
        {
            "credentials_url": "https://example.com/credentials",
            "credentials_url_secret": "refresh-me",
        },
    )

    assert seen["count"] == 1
    assert storage.access_key_id == "first-key"

    storage.credentials_expiration = datetime.now(timezone.utc) - timedelta(seconds=1)

    await storage.receive_upload("test.txt", _bytes_stream(b"hello"), "text/plain")

    assert seen["count"] == 2
    assert storage.access_key_id == "second-key"
    assert storage.session_token == "second-token"
    assert await storage.read_file("test.txt") == b"hello"


@pytest.mark.asyncio
async def test_receive_upload_and_read(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    content = b"hello world"
    meta = await storage.receive_upload(
        "test/hello.txt", _bytes_stream(content), "text/plain"
    )

    assert meta.path == "test/hello.txt"
    assert meta.filename == "hello.txt"
    assert meta.content_type == "text/plain"
    assert meta.size == len(content)
    assert meta.content_hash == "sha256:" + hashlib.sha256(content).hexdigest()

    # Read it back
    data = await storage.read_file("test/hello.txt")
    assert data == content


@pytest.mark.asyncio
async def test_get_file_metadata(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    await storage.receive_upload(
        "docs/report.pdf", _bytes_stream(b"pdf-content"), "application/pdf"
    )

    meta = await storage.get_file_metadata("docs/report.pdf")
    assert meta is not None
    assert meta.path == "docs/report.pdf"
    assert meta.filename == "report.pdf"
    assert meta.size == len(b"pdf-content")
    assert meta.content_type == "application/pdf"


@pytest.mark.asyncio
async def test_get_file_metadata_missing(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    meta = await storage.get_file_metadata("nonexistent.txt")
    assert meta is None


@pytest.mark.asyncio
async def test_read_file_missing(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    with pytest.raises(FileNotFoundError):
        await storage.read_file("nonexistent.txt")


@pytest.mark.asyncio
async def test_stream_file(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    content = b"hello world streaming"
    await storage.receive_upload("stream.txt", _bytes_stream(content), "text/plain")

    chunks = []
    async for chunk in storage.stream_file("stream.txt"):
        chunks.append(chunk)
    assert b"".join(chunks) == content


@pytest.mark.asyncio
async def test_stream_file_missing(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    with pytest.raises(FileNotFoundError):
        async for _ in storage.stream_file("nonexistent.txt"):
            pass


@pytest.mark.asyncio
async def test_delete_file(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    await storage.receive_upload("to-delete.txt", _bytes_stream(b"bye"), "text/plain")
    meta = await storage.get_file_metadata("to-delete.txt")
    assert meta is not None

    await storage.delete_file("to-delete.txt")
    meta = await storage.get_file_metadata("to-delete.txt")
    assert meta is None


@pytest.mark.asyncio
async def test_list_files(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    await storage.receive_upload("a.txt", _bytes_stream(b"aaa"), "text/plain")
    await storage.receive_upload("b.txt", _bytes_stream(b"bbb"), "text/plain")
    await storage.receive_upload("c.txt", _bytes_stream(b"ccc"), "text/plain")

    files, cursor = await storage.list_files()
    assert len(files) == 3
    filenames = {f.filename for f in files}
    assert filenames == {"a.txt", "b.txt", "c.txt"}


@pytest.mark.asyncio
async def test_list_files_with_limit(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    for i in range(5):
        await storage.receive_upload(f"file{i}.txt", _bytes_stream(b"x"), "text/plain")

    files, cursor = await storage.list_files(limit=3)
    assert len(files) == 3
    assert cursor is not None

    files2, cursor2 = await storage.list_files(limit=3, cursor=cursor)
    assert len(files2) == 2

    all_filenames = {f.filename for f in files + files2}
    assert all_filenames == {f"file{i}.txt" for i in range(5)}


@pytest.mark.asyncio
async def test_list_files_with_prefix_filter(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    await storage.receive_upload("images/cat.jpg", _bytes_stream(b"cat"), "image/jpeg")
    await storage.receive_upload("images/dog.jpg", _bytes_stream(b"dog"), "image/jpeg")
    await storage.receive_upload(
        "docs/readme.txt", _bytes_stream(b"readme"), "text/plain"
    )

    files, _ = await storage.list_files(prefix="images/")
    assert len(files) == 2
    filenames = {f.filename for f in files}
    assert filenames == {"cat.jpg", "dog.jpg"}


@pytest.mark.asyncio
async def test_prefix_is_prepended(s3_mock):
    await _create_bucket()
    storage = await _make_storage(prefix="uploads/")

    await storage.receive_upload("test.txt", _bytes_stream(b"hello"), "text/plain")

    # Verify the file is at the prefixed key in S3
    session = aioboto3.Session()
    async with session.client("s3", region_name=REGION) as client:
        resp = await client.get_object(Bucket=BUCKET, Key="uploads/test.txt")
        body = await resp["Body"].read()
        assert body == b"hello"

    # Storage methods should work with unprefixed paths
    data = await storage.read_file("test.txt")
    assert data == b"hello"

    meta = await storage.get_file_metadata("test.txt")
    assert meta is not None
    assert meta.path == "test.txt"

    files, _ = await storage.list_files()
    assert len(files) == 1
    assert files[0].path == "test.txt"


@pytest.mark.asyncio
async def test_prefix_trailing_slash_normalized(s3_mock):
    await _create_bucket()
    storage = await _make_storage(prefix="uploads")  # no trailing slash

    await storage.receive_upload("test.txt", _bytes_stream(b"hello"), "text/plain")

    # Verify the file is at the correctly prefixed key in S3
    session = aioboto3.Session()
    async with session.client("s3", region_name=REGION) as client:
        resp = await client.get_object(Bucket=BUCKET, Key="uploads/test.txt")
        body = await resp["Body"].read()
        assert body == b"hello"

    # Storage methods should work with unprefixed paths
    data = await storage.read_file("test.txt")
    assert data == b"hello"

    meta = await storage.get_file_metadata("test.txt")
    assert meta is not None
    assert meta.path == "test.txt"

    files, _ = await storage.list_files()
    assert len(files) == 1
    assert files[0].path == "test.txt"


@pytest.mark.asyncio
async def test_download_url(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    await storage.receive_upload("photo.jpg", _bytes_stream(b"jpeg-data"), "image/jpeg")

    url = await storage.download_url("photo.jpg")
    assert BUCKET in url
    assert "photo.jpg" in url


@pytest.mark.asyncio
async def test_capabilities():
    storage = S3Storage()
    caps = storage.capabilities
    assert caps.can_upload is True
    assert caps.can_delete is True
    assert caps.can_list is True
    assert caps.can_generate_signed_urls is True
    assert caps.requires_proxy_download is False


@pytest.mark.asyncio
async def test_storage_type():
    storage = S3Storage()
    assert storage.storage_type == "s3"


@pytest.mark.asyncio
async def test_configure_credentials_url_404_does_not_block_startup(
    monkeypatch, s3_mock
):
    """A 404 from the credentials URL should not prevent configure() from completing."""

    def fake_urlopen(request):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setattr("datasette_files_s3.urlopen", fake_urlopen)

    storage = S3Storage()
    # Should not raise
    await storage.configure(
        {
            "credentials_url": "https://example.com/credentials",
            "credentials_url_secret": "secret value",
        },
    )
    # Session should be None since credentials were not fetched
    assert storage.session is None


@pytest.mark.asyncio
async def test_end_to_end_with_datasette(s3_mock, tmp_path):
    await _create_bucket()

    datasette = Datasette(
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-s3": {
                            "storage": "s3",
                            "config": {
                                "bucket": BUCKET,
                                "region": REGION,
                            },
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
                "files-upload": True,
            },
        },
    )

    # Wait for startup to complete
    response = await datasette.client.get("/-/files/sources.json")
    assert response.status_code == 200
    sources = response.json()["sources"]
    s3_sources = [s for s in sources if s["storage_type"] == "s3"]
    assert len(s3_sources) == 1
    assert s3_sources[0]["slug"] == "test-s3"
    assert s3_sources[0]["capabilities"]["can_generate_signed_urls"] is True

    # Upload a file via the 3-step API
    file_content = b"hello from s3"
    prepare = await datasette.client.post(
        "/-/files/upload/test-s3/-/prepare",
        content=json.dumps(
            {
                "filename": "hello.txt",
                "content_type": "text/plain",
                "size": len(file_content),
            }
        ),
        headers={"Content-Type": "application/json"},
    )
    assert prepare.status_code == 200
    token = prepare.json()["upload_token"]
    upload_url = prepare.json()["upload_url"]

    upload = await datasette.client.post(
        upload_url,
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="upload_token"\r\n'
            b"\r\n" + token.encode() + b"\r\n"
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="hello.txt"\r\n'
            b"Content-Type: text/plain\r\n"
            b"\r\n" + file_content + b"\r\n"
            b"--boundary--\r\n"
        ),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
    )
    assert upload.status_code == 200

    complete = await datasette.client.post(
        "/-/files/upload/test-s3/-/complete",
        content=json.dumps({"upload_token": token}),
        headers={"Content-Type": "application/json"},
    )
    assert complete.status_code == 201
    data = complete.json()["file"]
    assert data["filename"] == "hello.txt"
    assert data["content_type"] == "text/plain"
    assert data["size"] == len(file_content)
    file_id = data["id"]
    assert file_id.startswith("df-")

    # Get file metadata via JSON API
    response = await datasette.client.get(f"/-/files/{file_id}.json")
    assert response.status_code == 200
    meta = response.json()
    assert meta["filename"] == "hello.txt"
    assert meta["content_type"] == "text/plain"

    # Download redirects to presigned URL for S3
    response = await datasette.client.get(
        f"/-/files/{file_id}/download", follow_redirects=False
    )
    assert response.status_code == 302
    assert BUCKET in response.headers["location"]


@pytest.mark.asyncio
async def test_upload_via_api_endpoints(s3_mock, tmp_path):
    """Upload via the 3-step HTTP API, which streams chunks to receive_upload."""
    await _create_bucket()

    datasette = Datasette(
        config={
            "plugins": {
                "datasette-files": {
                    "sources": {
                        "test-s3": {
                            "storage": "s3",
                            "config": {
                                "bucket": BUCKET,
                                "region": REGION,
                            },
                        }
                    }
                }
            },
            "permissions": {
                "files-browse": True,
                "files-upload": True,
            },
        },
    )

    # Ensure startup is complete
    response = await datasette.client.get("/-/files/sources.json")
    assert response.status_code == 200

    file_content = b"uploaded via streaming API"

    # Step 1: Prepare
    prepare = await datasette.client.post(
        "/-/files/upload/test-s3/-/prepare",
        content=json.dumps(
            {
                "filename": "stream-test.txt",
                "content_type": "text/plain",
                "size": len(file_content),
            }
        ),
        headers={"Content-Type": "application/json"},
    )
    assert prepare.status_code == 200
    token = prepare.json()["upload_token"]
    upload_url = prepare.json()["upload_url"]

    # Step 2: Upload (multipart form)
    upload = await datasette.client.post(
        upload_url,
        content=(
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="upload_token"\r\n'
            b"\r\n" + token.encode() + b"\r\n"
            b"--boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="stream-test.txt"\r\n'
            b"Content-Type: text/plain\r\n"
            b"\r\n" + file_content + b"\r\n"
            b"--boundary--\r\n"
        ),
        headers={"Content-Type": "multipart/form-data; boundary=boundary"},
    )
    assert upload.status_code == 200, upload.text

    # Step 3: Complete
    complete = await datasette.client.post(
        "/-/files/upload/test-s3/-/complete",
        content=json.dumps({"upload_token": token}),
        headers={"Content-Type": "application/json"},
    )
    assert complete.status_code == 201, complete.text
    file_data = complete.json()["file"]
    assert file_data["filename"] == "stream-test.txt"
    assert file_data["size"] == len(file_content)
    assert file_data["content_hash"].startswith("sha256:")
