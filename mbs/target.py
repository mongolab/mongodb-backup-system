__author__ = 'abdul'

import traceback

import mbs_logging
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import os

from utils import document_pretty_string

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
# Target Classes
###############################################################################
class BackupTarget(object):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def put_file(self, file_path):
        pass

    ###########################################################################
    def to_document(self):
        pass

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())

###############################################################################
# S3BucketTarget
###############################################################################
class S3BucketTarget(BackupTarget):

    ###########################################################################
    def __init__(self):
        BackupTarget.__init__(self)
        self._bucket_name = None
        self._access_key = None
        self._secret_key = None

    ###########################################################################
    def put_file(self, file_path):
        try:

            logger.info("S3BucketTarget: Uploading %s to s3 bucket %s" %
                        (file_path, self.bucket_name))
            conn = S3Connection(self.access_key, self.secret_key)
            bucket = conn.get_bucket(self.bucket_name)

            k = Key(bucket)
            file_key = os.path.basename(file_path)
            k.key = file_key
            file_obj = open(file_path)
            k.set_contents_from_file(file_obj)

            logger.info("S3BucketTarget: Uploading to s3 bucket %s completed"
                        " successfully!!" % self.bucket_name)
        except Exception, e:
            raise Exception("S3BucketTarget: Error while trying to upload '%s'"
                            " to s3 bucket %s. Cause: %s" %
                            (file_path, self.bucket_name, e))

    ###########################################################################
    @property
    def bucket_name(self):
        return self._bucket_name

    @bucket_name.setter
    def bucket_name(self, bucket_name):
        self._bucket_name = bucket_name

    ###########################################################################
    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, access_key):
        self._access_key = access_key

    ###########################################################################
    @property
    def secret_key(self):
        return self._secret_key

    @secret_key.setter
    def secret_key(self, secret_key):
        self._secret_key = secret_key

    def to_document(self):
        return {
            "_type": "S3BucketTarget",
            "bucketName": self.bucket_name,
            "accessKey": self.access_key,
            "secretKey": self.secret_key
        }