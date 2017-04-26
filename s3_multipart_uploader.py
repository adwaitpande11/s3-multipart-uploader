#!/usr/bin/env python
# Copyright 2017 by Jeremy Nation <jeremy@jeremynation.me>
# License: MIT.
import argparse
import base64
from functools import partial
import hashlib
import math
import os
import shutil
import tempfile
import textwrap

import boto3

# File piece size should be at least 5*1024*1024 bytes (5 MB).
# See: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html#errorCompleteMPU
DEFAULT_FILE_PIECE_SIZE = 10*1024*1024


def get_file_hash(filename, algorithm='md5'):
    """Linux equivalent: openssl <algorithm> -binary <filename> | base64"""
    hash_obj = getattr(hashlib, algorithm)()
    with open(filename, 'rb') as file_:
        read_chunk = partial(file_.read, hash_obj.block_size * 1024)
        for chunk in iter(read_chunk, ''):
            hash_obj.update(chunk)
    file_hash = hash_obj.digest()
    file_hash = base64.b64encode(file_hash).decode()
    return file_hash


def split_input_file(input_filename, temp_dir, file_piece_size):
    input_file_size = os.path.getsize(input_filename)
    num_file_pieces = int(math.ceil(input_file_size / float(file_piece_size)))
    file_piece_names = []
    with open(input_filename, 'rb') as input_file:
        for index in range(num_file_pieces):
            this_file_piece_name = os.path.join(
                temp_dir,
                '.'.join((
                    os.path.basename(input_filename),
                    str(index+1).zfill(len(str(num_file_pieces))),
                )),
            )
            with open(this_file_piece_name, 'wb') as this_file_piece:
                this_file_piece.write(input_file.read(file_piece_size))
            file_piece_names.append(this_file_piece_name)
    return file_piece_names


def upload_file_pieces(
        bucket_name,
        key,
        expected_complete_file_hash,
        expected_complete_file_size,
        file_piece_names):

    s3 = boto3.client('s3')

    create_mpu_response = s3.create_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        Metadata={'md5': expected_complete_file_hash},
    )
    upload_id = create_mpu_response['UploadId']

    fileparts = {'Parts': []}

    complete_mpu_response = {}
    try:
        for index, file_piece_name in enumerate(file_piece_names):
            part_num = index + 1
            file_piece_hash = get_file_hash(file_piece_name)
            print(
                "Uploading %(file_piece_name)s (part %(part_num)s of "
                "%(total_num)s), hash: %(file_piece_hash)s " % {
                    'file_piece_name': file_piece_name,
                    'part_num': part_num,
                    'total_num': len(file_piece_names),
                    'file_piece_hash': file_piece_hash,
                }
            )
            with open(file_piece_name, 'rb') as file_piece:
                upload_part_response = s3.upload_part(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=file_piece,
                    ContentMD5=file_piece_hash,
                )
            fileparts['Parts'].append({
                'ETag': upload_part_response['ETag'],
                'PartNumber': int(part_num),
            })
        print("Finished uploading.")

        complete_mpu_response = s3.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload=fileparts,
        )
    finally:
        if 'ETag' not in complete_mpu_response:
            print(
                "Exiting program and the upload is incomplete. Aborting "
                "multipart upload..."
            )
            s3.abort_multipart_upload(
                Bucket=bucket_name, Key=key, UploadId=upload_id)
            print('Upload aborted.')

    head_object_response = s3.head_object(Bucket=bucket_name, Key=key)
    assertions = (
        {
            'description': 'hashes for combined files are equal',
            'expected': expected_complete_file_hash,
            'found': head_object_response['Metadata']['md5'],
        },
        {
            'description': 'file sizes of combined files are equal',
            'expected': expected_complete_file_size,
            'found': head_object_response['ContentLength'],
        },
    )
    for assertion in assertions:
        assert assertion['expected'] == assertion['found'], \
               "Failed check '%s'!" % assertion['description']
        print("Passed check '%s'." % assertion['description'])
    print('Upload successful!')


def upload_file(
        bucket_name, original_filename, file_piece_size, keep_file_pieces):
    temp_dir = tempfile.mkdtemp()
    try:
        file_piece_names = split_input_file(
            original_filename,
            temp_dir,
            file_piece_size,
        )

        upload_file_pieces(
            bucket_name=bucket_name,
            key=os.path.basename(original_filename),
            expected_complete_file_hash=get_file_hash(original_filename),
            expected_complete_file_size=os.path.getsize(original_filename),
            file_piece_names=file_piece_names,
        )
    finally:
        if not keep_file_pieces:
            print("Deleting file piece directory %s..." % temp_dir)
            shutil.rmtree(temp_dir)
            print("Deleted %s successfully." % temp_dir)
        else:
            print(
                "Leaving file piece directory %s at user request." % temp_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""
            Do a multipart file upload to AWS S3 with file integrity checking. See
            https://aws.amazon.com/premiumsupport/knowledge-center/s3-multipart-upload-cli/
            for more information. Be sure to configure your AWS credentials with
            'aws configure' or similar before running this program.
        """),
    )
    parser.add_argument(
        'bucket_name',
        metavar='bucket-name',
        help='name of destination S3 bucket',
    )
    parser.add_argument(
        'original_filename',
        metavar='original-filename',
        help='name of file to upload',
    )
    parser.add_argument(
        '--file-piece-size',
        default=DEFAULT_FILE_PIECE_SIZE,
        help='max size in bytes of each file piece',
        type=int,
    )
    parser.add_argument(
        '--keep-file-pieces',
        action='store_true',
        help='keep file pieces after program is finished',
    )
    args = parser.parse_args()
    upload_file(
        args.bucket_name,
        args.original_filename,
        args.file_piece_size,
        args.keep_file_pieces,
    )
