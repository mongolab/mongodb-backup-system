import os

from uuid import uuid4

import boto
import boto.auth

from boto.s3.connection import HostRequiredError

import mbs.s3_utils as s3_utils

from . import BaseTest, ENV
from .tutils import truthy


###############################################################################
# S3UtilsTest
###############################################################################
class S3UtilsTest(BaseTest):

    ###########################################################################
    def setUp(self):
        super(S3UtilsTest, self).setUp()

        self._check_run_int_tests_else_skip()

        self._key_id = self._get_env_var_or_skip('S3_UTILS_TEST_KEY_ID')
        self._secret_key = self._get_env_var_or_skip('S3_UTILS_TEST_SECRET_KEY')
        self._request = None
        self._response = None
        self._error = None

    ###########################################################################
    def _test_crud(self, bucket_name):
        con, bucket, _ = \
            s3_utils.get_connection_for_bucket(
                self._key_id, self._secret_key, bucket_name)
        k = bucket.new_key('%s-unittest' % (uuid4()))
        try:
            k.set_contents_from_string(k.name)
            self.assertEqual(k.get_contents_as_string(), k.name)
            self.assertSequenceEqual(
                [k_.name for k_ in bucket.list(k.name)], [k.name])
        finally:
            k.delete()

    ###########################################################################
    def test_eu_central_crud(self):
        self._test_crud(
            self._get_env_var_or_skip(
                'S3_UTILS_TEST_EU_CENTRAL_1_BUCKET_NAME'))

    ###########################################################################
    def test_eu_west_crud(self):
        self._test_crud(
            self._get_env_var_or_skip(
                'S3_UTILS_TEST_EU_WEST_1_BUCKET_NAME'))

    ###########################################################################
    def test_us_east_crud(self):
        self._test_crud(
            self._get_env_var_or_skip(
                'S3_UTILS_TEST_US_EAST_1_BUCKET_NAME'))

    ###########################################################################
    def test_us_west_crud(self):
        self._test_crud(
            self._get_env_var_or_skip(
                'S3_UTILS_TEST_US_WEST_2_BUCKET_NAME'))

    ###########################################################################
    def handle_request_data(self, request, response, error=0):
        self._request = request
        self._response = response
        self._error = error

    ###########################################################################
    def test_sig_v4_upgrade(self):
        bucket_name = \
            self._get_env_var_or_skip(
                'S3_UTILS_TEST_EU_CENTRAL_1_BUCKET_NAME')
        con, bucket, region = \
            s3_utils.get_connection_for_bucket(
                self._key_id, self._secret_key, bucket_name)
        con.set_request_hook(self)
        bucket = con.get_bucket(bucket_name)
        # XXX: fragile test
        assert(isinstance(con._auth_handler, boto.auth.S3HmacAuthV4Handler))

    ###########################################################################
    def test_sig_v2_downgrade_indirect(self):
        bucket_name = \
            self._get_env_var_or_skip('S3_UTILS_TEST_US_WEST_2_BUCKET_NAME')
        con = s3_utils.get_connection(self._key_id, self._secret_key)
        con.set_request_hook(self)
        bucket = con.get_bucket(bucket_name)
        # XXX: fragile test
        assert(isinstance(con._auth_handler, boto.auth.HmacAuthV1Handler))

    ###########################################################################
    def test_sig_v4_direct(self):
        bucket_name = \
            self._get_env_var_or_skip('S3_UTILS_TEST_US_WEST_2_BUCKET_NAME')
        use_sigv4 = boto.config.get('s3', 'use-sigv4', '')
        try:
            try:
                boto.config.set('s3', 'use-sigv4', 'True')
            except:
                boto.config.add_section('s3')
                boto.config.set('s3', 'use-sigv4', 'True')
            self.assertRaises(HostRequiredError,
                              s3_utils.get_connection,
                              self._key_id,
                              self._secret_key)
            con = s3_utils.get_connection(self._key_id, self._secret_key, 'us-west-2')
            con.set_request_hook(self)
            bucket = con.get_bucket(bucket_name)
            # XXX: fragile test
            assert(isinstance(con._auth_handler, boto.auth.S3HmacAuthV4Handler))
        finally:
            boto.config.set('s3', 'use-sigv4', use_sigv4)


