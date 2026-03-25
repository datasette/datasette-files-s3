import asyncio
import hashlib
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator, Optional
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

logger = logging.getLogger("datasette_files_s3")

import aioboto3
from botocore.exceptions import ClientError
from datasette import hookimpl
from datasette_files.base import FileMetadata, Storage, StorageCapabilities


class S3Storage(Storage):
    storage_type = "s3"
    capabilities = StorageCapabilities(
        can_upload=True,
        can_delete=True,
        can_list=True,
        can_generate_signed_urls=True,
        requires_proxy_download=False,
    )

    async def configure(self, config: dict, get_secret=None) -> None:
        self.bucket = config.get("bucket")
        self.prefix = self._normalize_prefix(config.get("prefix", ""))
        self.region = config.get("region", "us-east-1")
        self.endpoint_url = config.get("endpoint_url")
        self.credentials_url = config.get("credentials_url")
        self.credentials_url_secret = config.get("credentials_url_secret")
        self.s3_folder = None

        self.access_key_id = config.get("access_key_id")
        self.secret_access_key = config.get("secret_access_key")
        self.session_token = config.get("session_token")
        self.credentials_expiration = None
        self.session = None
        self._credentials_lock = asyncio.Lock()

        if not self.bucket and not self.credentials_url:
            raise ValueError("S3 storage requires either bucket or credentials_url")

        try:
            await self._ensure_credentials(force=True)
        except Exception as e:
            logger.warning(
                "Failed to fetch initial S3 credentials from %s: %s — "
                "will retry when credentials are needed",
                self.credentials_url,
                e,
            )

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        if prefix and not prefix.endswith("/"):
            return prefix + "/"
        return prefix

    @staticmethod
    def _parse_expiration(value: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as ex:
            raise ValueError(f"Invalid Expiration value: {value!r}") from ex
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    @classmethod
    def _parse_s3_folder(cls, value: str) -> tuple[str, str]:
        parsed = urlparse(value)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"Invalid S3Folder value: {value!r}")
        prefix = cls._normalize_prefix(parsed.path.lstrip("/"))
        return parsed.netloc, prefix

    def _session_is_expired(self) -> bool:
        return (
            self.credentials_expiration is not None
            and datetime.now(timezone.utc) >= self.credentials_expiration
        )

    def _rebuild_session(self) -> None:
        self.session = aioboto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            aws_session_token=self.session_token,
            region_name=self.region,
        )

    def _fetch_credentials_sync(self) -> dict:
        if not self.credentials_url_secret:
            raise ValueError(
                "credentials_url_secret is required when credentials_url is configured"
            )
        body = urlencode({"secret": self.credentials_url_secret}).encode("utf-8")
        request = Request(
            self.credentials_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))

    async def _refresh_credentials(self) -> None:
        payload = await asyncio.to_thread(self._fetch_credentials_sync)

        self.access_key_id = payload["AccessKeyId"]
        self.secret_access_key = payload["SecretAccessKey"]
        self.session_token = payload["SessionToken"]
        self.credentials_expiration = self._parse_expiration(payload["Expiration"])
        self.s3_folder = payload["S3Folder"]
        self.bucket, self.prefix = self._parse_s3_folder(self.s3_folder)
        self._rebuild_session()

    async def _ensure_credentials(self, force: bool = False) -> None:
        async with self._credentials_lock:
            if self.credentials_url:
                if force or self.session is None or self._session_is_expired():
                    await self._refresh_credentials()
            elif force or self.session is None:
                self._rebuild_session()

    def _key(self, path: str) -> str:
        return f"{self.prefix}{path}" if self.prefix else path

    @asynccontextmanager
    async def _client(self):
        await self._ensure_credentials()
        kwargs = {}
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        async with self.session.client("s3", **kwargs) as client:
            yield client

    async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
        async with self._client() as client:
            try:
                resp = await client.head_object(Bucket=self.bucket, Key=self._key(path))
                return FileMetadata(
                    path=path,
                    filename=path.split("/")[-1],
                    content_type=resp.get("ContentType"),
                    size=resp.get("ContentLength"),
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return None
                raise

    async def read_file(self, path: str) -> bytes:
        async with self._client() as client:
            try:
                resp = await client.get_object(Bucket=self.bucket, Key=self._key(path))
                return await resp["Body"].read()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError(f"File not found: {path}")
                raise

    async def stream_file(self, path: str) -> AsyncIterator[bytes]:
        async with self._client() as client:
            try:
                resp = await client.get_object(Bucket=self.bucket, Key=self._key(path))
                body = resp["Body"]
                while True:
                    chunk = await body.read(65536)
                    if not chunk:
                        break
                    yield chunk
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError(f"File not found: {path}")
                raise

    async def read_bytes(self, path: str, num_bytes: int = 2048) -> bytes:
        async with self._client() as client:
            try:
                resp = await client.get_object(
                    Bucket=self.bucket,
                    Key=self._key(path),
                    Range=f"bytes=0-{num_bytes - 1}",
                )
                return await resp["Body"].read()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError(f"File not found: {path}")
                raise

    async def receive_upload(
        self, path: str, stream: AsyncIterator[bytes], content_type: str
    ) -> FileMetadata:
        # Collect chunks, computing hash incrementally.
        # S3 put_object requires the full body; true streaming would
        # need S3 multipart upload, which can be added later.
        chunks = []
        sha256 = hashlib.sha256()
        size = 0
        async for chunk in stream:
            chunks.append(chunk)
            sha256.update(chunk)
            size += len(chunk)
        content = b"".join(chunks)
        async with self._client() as client:
            await client.put_object(
                Bucket=self.bucket,
                Key=self._key(path),
                Body=content,
                ContentType=content_type,
            )
        return FileMetadata(
            path=path,
            filename=path.split("/")[-1],
            content_type=content_type,
            content_hash="sha256:" + sha256.hexdigest(),
            size=size,
        )

    async def delete_file(self, path: str) -> None:
        async with self._client() as client:
            await client.delete_object(Bucket=self.bucket, Key=self._key(path))

    async def list_files(
        self,
        prefix: str = "",
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> tuple[list[FileMetadata], Optional[str]]:
        async with self._client() as client:
            kwargs = {
                "Bucket": self.bucket,
                "Prefix": self._key(prefix),
                "MaxKeys": limit,
            }
            if cursor:
                kwargs["ContinuationToken"] = cursor
            resp = await client.list_objects_v2(**kwargs)
            files = []
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                # Strip our prefix to get the storage-relative path
                path = key.removeprefix(self.prefix) if self.prefix else key
                files.append(
                    FileMetadata(
                        path=path,
                        filename=key.split("/")[-1],
                        size=obj.get("Size"),
                    )
                )
            next_cursor = resp.get("NextContinuationToken")
            return files, next_cursor

    async def download_url(self, path: str, expires_in: int = 300) -> str:
        async with self._client() as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": self._key(path)},
                ExpiresIn=expires_in,
            )


@hookimpl
def register_files_storage_types(datasette):
    return [S3Storage]
