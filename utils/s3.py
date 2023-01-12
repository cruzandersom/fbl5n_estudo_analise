from io import BytesIO, StringIO
from typing import Union


def get_file_from_s3(s3_client, bucket: str, key: str) -> bytes:
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    return response['Body'].read()


def send_file_to_s3(s3_client, bucket: str, key: str, buffer: Union[BytesIO, StringIO]) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )


def copy_object_in_s3(s3_client, bucket: str, from_key: str, to_key: str):
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': from_key},
        Key=to_key
    )


def delete_file_from_s3(s3_client, bucket: str, key: str):
    s3_client.delete_object(
        Bucket=bucket,
        Key=key
    )
