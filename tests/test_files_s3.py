import hashlib

import aioboto3
import pytest
from aiomoto import mock_aws
from datasette.app import Datasette

from datasette_files_s3 import S3Storage

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
        get_secret=None,
    )
    return storage


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
        get_secret=None,
    )
    assert storage.bucket == "my-bucket"
    assert storage.prefix == "uploads/"
    assert storage.region == "eu-west-1"


@pytest.mark.asyncio
async def test_configure_defaults(s3_mock):
    storage = S3Storage()
    await storage.configure({"bucket": "my-bucket"}, get_secret=None)
    assert storage.prefix == ""
    assert storage.region == "us-east-1"


@pytest.mark.asyncio
async def test_configure_with_get_secret(s3_mock):
    secrets = {
        "AWS_ACCESS_KEY_ID": "secret-key-id",
        "AWS_SECRET_ACCESS_KEY": "secret-access-key",
    }

    async def get_secret(name):
        return secrets.get(name)

    storage = S3Storage()
    await storage.configure({"bucket": "my-bucket"}, get_secret=get_secret)
    assert storage.access_key_id == "secret-key-id"
    assert storage.secret_access_key == "secret-access-key"


@pytest.mark.asyncio
async def test_configure_config_overrides_secrets(s3_mock):
    async def get_secret(name):
        return "from-secrets"

    storage = S3Storage()
    await storage.configure(
        {
            "bucket": "my-bucket",
            "access_key_id": "from-config",
            "secret_access_key": "from-config-secret",
        },
        get_secret=get_secret,
    )
    assert storage.access_key_id == "from-config"
    assert storage.secret_access_key == "from-config-secret"


@pytest.mark.asyncio
async def test_receive_upload_and_read(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    content = b"hello world"
    meta = await storage.receive_upload("test/hello.txt", content, "text/plain")

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

    await storage.receive_upload("docs/report.pdf", b"pdf-content", "application/pdf")

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
async def test_delete_file(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    await storage.receive_upload("to-delete.txt", b"bye", "text/plain")
    meta = await storage.get_file_metadata("to-delete.txt")
    assert meta is not None

    await storage.delete_file("to-delete.txt")
    meta = await storage.get_file_metadata("to-delete.txt")
    assert meta is None


@pytest.mark.asyncio
async def test_list_files(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    await storage.receive_upload("a.txt", b"aaa", "text/plain")
    await storage.receive_upload("b.txt", b"bbb", "text/plain")
    await storage.receive_upload("c.txt", b"ccc", "text/plain")

    files, cursor = await storage.list_files()
    assert len(files) == 3
    filenames = {f.filename for f in files}
    assert filenames == {"a.txt", "b.txt", "c.txt"}


@pytest.mark.asyncio
async def test_list_files_with_limit(s3_mock):
    await _create_bucket()
    storage = await _make_storage()

    for i in range(5):
        await storage.receive_upload(f"file{i}.txt", b"x", "text/plain")

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

    await storage.receive_upload("images/cat.jpg", b"cat", "image/jpeg")
    await storage.receive_upload("images/dog.jpg", b"dog", "image/jpeg")
    await storage.receive_upload("docs/readme.txt", b"readme", "text/plain")

    files, _ = await storage.list_files(prefix="images/")
    assert len(files) == 2
    filenames = {f.filename for f in files}
    assert filenames == {"cat.jpg", "dog.jpg"}


@pytest.mark.asyncio
async def test_prefix_is_prepended(s3_mock):
    await _create_bucket()
    storage = await _make_storage(prefix="uploads/")

    await storage.receive_upload("test.txt", b"hello", "text/plain")

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

    await storage.receive_upload("test.txt", b"hello", "text/plain")

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

    await storage.receive_upload("photo.jpg", b"jpeg-data", "image/jpeg")

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

    # Upload a file
    response = await datasette.client.post(
        "/-/files/upload/test-s3",
        files={"file": ("hello.txt", b"hello from s3", "text/plain")},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["filename"] == "hello.txt"
    assert data["content_type"] == "text/plain"
    assert data["size"] == len(b"hello from s3")
    file_id = data["file_id"]
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
