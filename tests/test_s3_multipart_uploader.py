#!/usr/bin/env python
# Copyright 2017 by Jeremy Nation <jeremy@jeremynation.me>
# License: MIT.
import os
import shutil
import tempfile
import unittest

import boto3
from moto import mock_s3

from s3_multipart_uploader import (
    get_file_hash,
    split_input_file,
    upload_file,
)


class TestS3MultipartUploader(unittest.TestCase):

    # 50 bytes
    SMALL_TESTFILE_CONTENT = "+ELokXtvOjByfb92hqVRE74SOaA0B2AS3iwtPkjv74HTY76sqt"
    SMALL_TESTFILE_PATH = os.path.join(os.curdir, 'tests', 'small_testfile')

    BIG_TESTFILE_SIZE = 6*1024*1024
    BIG_TESTFILE_PATH = os.path.join(os.curdir, 'tests', 'big_testfile')

    def setUp(self):
        testfile = open(self.SMALL_TESTFILE_PATH, 'wb')
        testfile.write(self.SMALL_TESTFILE_CONTENT)

    def tearDown(self):
        os.remove(self.SMALL_TESTFILE_PATH)

    def test_get_file_hash_default_algorithm_is_md5(self):
        self.assertEqual(
            get_file_hash(self.SMALL_TESTFILE_PATH),
            get_file_hash(self.SMALL_TESTFILE_PATH, algorithm='md5'),
        )

    def test_get_file_hash_md5(self):
        self.assertEqual(
            get_file_hash(self.SMALL_TESTFILE_PATH),
            'CoM0C8BkxBNJJJyzEO+PYw==',
        )

    def test_get_file_hash_sha256(self):
        self.assertEqual(
            get_file_hash(self.SMALL_TESTFILE_PATH, algorithm='sha256'),
            '2GMyryXzElFw2g5yZxpPEU8dgoIRv9FNHoZeTSKU67s=',
        )

    def test_split_input_file(self):
        testfile_size = os.path.getsize(self.SMALL_TESTFILE_PATH)
        self.assertEqual(
            testfile_size % 5,
            0,
            "Size of test file '%s' is not divisible by 5" % self.SMALL_TESTFILE_PATH,
        )
        file_piece_size = testfile_size * 2 // 5
        temp_dir = tempfile.mkdtemp()
        try:
            file_piece_names = split_input_file(
                self.SMALL_TESTFILE_PATH, temp_dir, file_piece_size)
            self.assertEqual(
                os.path.getsize(file_piece_names[0]),
                testfile_size * 2 // 5,
            )
            self.assertEqual(
                os.path.getsize(file_piece_names[1]),
                testfile_size * 2 // 5,
            )
            self.assertEqual(
                os.path.getsize(file_piece_names[2]),
                testfile_size // 5,
            )
        finally:
            shutil.rmtree(temp_dir)

    @mock_s3
    def test_upload_file(self):
        with open(self.BIG_TESTFILE_PATH, 'wb') as testfile:
            testfile.write(os.urandom(self.BIG_TESTFILE_SIZE))
        try:
            test_bucket_name = 'moto-test-bucket'
            conn = boto3.resource('s3')
            conn.create_bucket(Bucket=test_bucket_name)

            upload_file(
                bucket_name=test_bucket_name,
                original_filename=self.BIG_TESTFILE_PATH,
                file_piece_size=5*1024*1024 + 100,
                keep_file_pieces=False,
            )

            key = os.path.basename(self.BIG_TESTFILE_PATH)
            found_s3_obj = conn.Object(test_bucket_name, key)

            with open(self.BIG_TESTFILE_PATH, 'rb') as testfile:
                expected_content = testfile.read()
            found_content = found_s3_obj.get()['Body'].read()
            self.assertEqual(expected_content, found_content)

            expected_file_hash = get_file_hash(self.BIG_TESTFILE_PATH)
            found_file_hash = found_s3_obj.get()['Metadata']['md5']
            self.assertEqual(expected_file_hash, found_file_hash)
        finally:
            os.remove(self.BIG_TESTFILE_PATH)

if __name__ == '__main__':
    unittest.main()
