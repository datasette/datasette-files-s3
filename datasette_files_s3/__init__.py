import hashlib
from typing import Optional

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

    async def configure(self, config: dict, get_secret) -> None:
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "")
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"
        self.region = config.get("region", "us-east-1")
        self.endpoint_url = config.get("endpoint_url")

        # Try config first, then datasette-secrets, then fall back to default chain
        self.access_key_id = config.get("access_key_id")
        self.secret_access_key = config.get("secret_access_key")

        if get_secret is not None:
            if not self.access_key_id:
                self.access_key_id = await get_secret("AWS_ACCESS_KEY_ID")
            if not self.secret_access_key:
                self.secret_access_key = await get_secret("AWS_SECRET_ACCESS_KEY")

        self.session = aioboto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
        )

    def _key(self, path: str) -> str:
        return f"{self.prefix}{path}" if self.prefix else path

    def _client(self):
        kwargs = {}
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        return self.session.client("s3", **kwargs)

    async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
        async with self._client() as client:
            try:
                resp = await client.head_object(
                    Bucket=self.bucket, Key=self._key(path)
                )
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
                resp = await client.get_object(
                    Bucket=self.bucket, Key=self._key(path)
                )
                return await resp["Body"].read()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    raise FileNotFoundError(f"File not found: {path}")
                raise

    async def receive_upload(
        self, path: str, content: bytes, content_type: str
    ) -> FileMetadata:
        async with self._client() as client:
            await client.put_object(
                Bucket=self.bucket,
                Key=self._key(path),
                Body=content,
                ContentType=content_type,
            )
        content_hash = "sha256:" + hashlib.sha256(content).hexdigest()
        return FileMetadata(
            path=path,
            filename=path.split("/")[-1],
            content_type=content_type,
            content_hash=content_hash,
            size=len(content),
        )

    async def delete_file(self, path: str) -> None:
        async with self._client() as client:
            await client.delete_object(
                Bucket=self.bucket, Key=self._key(path)
            )

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
