# -*- coding: utf-8 -*-
"""S3 utils."""

import gzip
from typing import Optional

import boto3


def get_s3(aws_access_key, aws_secret_access_key):
    """Get S3 connections."""
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_access_key)
    return s3


def get_s3_resource(aws_access_key, aws_secret_access_key):
    """Get S3 resource."""
    s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_access_key)
    return s3_resource


def write_s3(env: str, source_system: str, source_subsystem: str, object_name: str, s3,
             s3_bucket: str, content, date_valid: str, page: int = 1,
             extension: str = 'json', compress: bool = True, s3_path: Optional[str] = None):
    """Write to S3."""
    s3_path = s3_path or ('{env}/{source_system}/{source_subsystem}/'
                          '{object_name}{date_valid}/data-{page}.{extension}')

    if compress:
        content = gzip.compress(content.encode('utf-8'))
        extension = extension + '.gz'

    s3_key = s3_path.format(
        env=env,
        source_system=source_system,
        source_subsystem=source_subsystem,
        object_name=object_name,
        date_valid='/{0}'.format(date_valid) if date_valid else '',
        page=page,
        extension=extension
    )
    print('Writing to {}'.format(s3_key))
    s3.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
