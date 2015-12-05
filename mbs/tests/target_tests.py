import hashlib
import math
import os

from tempfile import NamedTemporaryFile
from uuid import uuid4

from mock import patch, Mock

import mbs.target

from . import BaseTest
from .tutils import md5


###############################################################################
# TargetTest
###############################################################################
class TargetTest(BaseTest):

    ###########################################################################
    def test_multi_part_put(self):
        hash_ = hashlib.md5()
        mp_upload_mock = Mock(**{'upload_part_from_file.side_effect':
                                 lambda data, i: hash_.update(data.read()),
                                 'complete_upload': Mock()})
        with NamedTemporaryFile() as dump, \
             open('/dev/urandom', 'rb') as random_data, \
             patch.object(mbs.target, 'MAX_SPLIT_SIZE', 1024), \
             patch.object(mbs.target.S3BucketTarget,
                          '_get_bucket',
                          Mock(return_value=Mock(
                                **{'initiate_multipart_upload.return_value':
                                   mp_upload_mock}))):
            dump.write(random_data.read(10000))
            target = self.mbs.maker.make({'_type': 'S3BucketTarget'})
            target._multi_part_put(dump.name, 'com.foo.bar', 10000)

            self.assertEqual(mp_upload_mock.upload_part_from_file.call_count,
                             math.ceil(10000/1024))
            self.assertTrue(mp_upload_mock.complete_upload.called)
            self.assertEqual(hash_.hexdigest(), md5(dump.name))

    def test_s3_validate(self):
        target = self.mbs.maker.make({
            '_type': 'S3BucketTarget',
        })
        self.assertEqual(len(target.validate()), 3)

        target = self.mbs.maker.make({
            '_type': 'S3BucketTarget',
            'bucketName': 'foo_bar',
            'accessKey': 'xxxx',
            'secretKey': 'xxxx',
        })
        self.assertEqual(len(target.validate()), 1)

        target = self.mbs.maker.make({
            '_type': 'S3BucketTarget',
            'bucketName': 'FOO',
            'accessKey': 'xxxx',
            'secretKey': 'xxxx',
        })
        self.assertEqual(len(target.validate()), 1)

        target = self.mbs.maker.make({
            '_type': 'S3BucketTarget',
            'bucketName': 'foo',
            'accessKey': 'xxxx',
            'secretKey': 'xxxx',
        })
        self.assertEqual(len(target.validate()), 0)

    def test_s3_has_sufficient_permissions(self):
        self._check_run_int_tests_else_skip()

        target = self.mbs.maker.make({
            '_type': 'S3BucketTarget',
            'bucketName': self._get_env_var_or_skip(
                                'S3_UTILS_TEST_US_WEST_2_BUCKET_NAME'),
            'accessKey': self._get_env_var_or_skip(
                                'S3_UTILS_TEST_KEY_ID'),
            'secretKey': self._get_env_var_or_skip(
                                'S3_UTILS_TEST_SECRET_KEY'),
        })
        target.has_sufficient_permissions()

        target = self.mbs.maker.make({
            '_type': 'S3BucketTarget',
            'bucketName': self._get_env_var_or_skip(
                                'S3_UTILS_TEST_US_WEST_2_BUCKET_NAME_NO_PERMS'),
            'accessKey': self._get_env_var_or_skip(
                                'S3_UTILS_TEST_KEY_ID'),
            'secretKey': self._get_env_var_or_skip(
                                'S3_UTILS_TEST_SECRET_KEY')
        })
        self.assertGreater(len(target.has_sufficient_permissions()), 0)

