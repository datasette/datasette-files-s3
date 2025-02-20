import aioboto3
from datasette import hookimpl
from datasette_files.base import Storage, File
from typing import AsyncGenerator, Optional


# @dataclass
# class File:
#     name: str
#     path: str
#     type: Optional[str]
#     mtime: Optional[int]
#     ctime: Optional[int]



class S3Storage(Storage):
    def __init__(self, name, s3_bucket, s3_prefix):
        self.name = name
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    async def list_files(self, last_token: str = None) -> AsyncGenerator[File, None]:
        session = aioboto3.Session()
        async with session.client('s3') as s3_client:
            paginator = s3_client.get_paginator('list_objects_v2')
            async for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix):
                for item in page.get('Contents', []):
                    print(item)
                    yield File(
                        name=item['Key'].split('/')[-1],
                        path=item['Key'],
                        type=None,
                        mtime=item['LastModified'],
                        ctime=None, # Not provided by S3
                    )

    async def upload_form_fields(self, file_name, file_type) -> dict:
        pass

    async def upload_complete(self, file_name, file_type):
        pass

    async def read_file(self, path: str) -> bytes:
        pass

    async def expiring_download_url(self, path: str, expires_after=5 * 60) -> str:
        s3_client = boto3.client('s3')
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.s3_bucket, 'Key': path},
            ExpiresIn=expires_after
        )


@hookimpl
def register_files_storages():
    return [
        # S3Storage("datasette-files-cors-bucket", ""),
        S3Storage("s3.static.niche-museums.com", "static.niche-museums.com", ""),
    ]
