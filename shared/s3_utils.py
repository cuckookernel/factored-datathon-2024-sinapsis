"""S3 manipulation helpers"""
import os
from dataclasses import dataclass
from http import HTTPStatus
from typing import TypeAlias

import boto3
import botocore
import botocore.client
import botocore.exceptions

RawS3Client: TypeAlias = "botocore.client.S3"  # type: ignore [name-defined]

@dataclass
class S3Ref:
    """pack together bucket and key references"""

    bucket: str
    key: str


class S3Client:
    """Simplify S3 interaction a bit"""

    def __init__(self, client: RawS3Client) -> None:
        self._client: RawS3Client = client

    def exists(self, ref: S3Ref) -> bool:
        """Check whether ref.key exists in ref.bucket"""
        try:
            self._client.head_object(Bucket=ref.bucket, Key=ref.key)

        except botocore.exceptions.ClientError as err:
            if "(404)" in err.args[0] :
                return False
            else:
                raise
        else:
            return True

    def get(self, ref: S3Ref) -> bytes:
        """Get contents of an object as raw bytes"""
        resp = self._client.get_object(Bucket=ref.bucket, Key=ref.key)
        return resp['Body'].read()


    def put(self, ref: S3Ref, data: bytes) -> bool:
        """Get bytes to an object on s3"""
        resp = self._client.put_object(Bucket=ref.bucket, Key=ref.key, Body=data)
        # print(resp)
        if resp['ResponseMetadata']['HTTPStatusCode'] == HTTPStatus.OK:
            return True
        else:
            raise RuntimeError(f"Something went wrong with put_object:\n{resp}")


def get_s3_client() -> S3Client:
    """Get a (wrapped) S3 Client"""
    # %%
    client = boto3.client(
        's3',
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    return S3Client(client)
    # %%
