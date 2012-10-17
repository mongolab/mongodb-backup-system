__author__ = 'abdul'

import traceback
import os

import mbs_logging
from utils import document_pretty_string

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.ec2 import EC2Connection

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

    ###########################################################################
    def is_valid(self):
        errors = self.validate()
        if errors:
            return False
        else:
            return True

    ###########################################################################
    def validate(self):
        """
         Returns an array containing validation messages (if any). Empty if no
         validation errors
        """
        return []

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

            return FileReference(file_key)
        except Exception, e:
            traceback.print_exc()
            msg = ("S3BucketTarget: Error while trying to upload '%s'"
                   " to s3 bucket %s. Cause: %s" %
                   (file_path, self.bucket_name, e))
            raise Exception(msg, e)

    ###########################################################################
    @property
    def bucket_name(self):
        return self._bucket_name

    @bucket_name.setter
    def bucket_name(self, bucket_name):
        self._bucket_name = str(bucket_name)

    ###########################################################################
    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, access_key):
        self._access_key = str(access_key)

    ###########################################################################
    @property
    def secret_key(self):
        return self._secret_key

    @secret_key.setter
    def secret_key(self, secret_key):
        self._secret_key = str(secret_key)

    ###########################################################################
    def to_document(self):
        return {
            "_type": "S3BucketTarget",
            "bucketName": self.bucket_name,
            "accessKey": self.access_key,
            "secretKey": self.secret_key
        }

    ###########################################################################
    def validate(self):
        errors = []

        if not self.bucket_name:
            errors.append("Missing 'bucketName' property")

        if not self.access_key:
            errors.append("Missing 'accessKey' property")

        if not self.secret_key:
            errors.append("Missing 'secretKey' property")

        return errors


###############################################################################
# EbsSnapshotTarget
###############################################################################
class EbsSnapshotTarget(BackupTarget):

    ###########################################################################
    def __init__(self):
        BackupTarget.__init__(self)
        self._access_key = None
        self._secret_key = None
        self._ec2_connection = None

    ###########################################################################
    def put_file(self, file_path):
        raise Exception("Unsupported operation")

    ###########################################################################
    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, access_key):
        self._access_key = str(access_key)

    ###########################################################################
    @property
    def secret_key(self):
        return self._secret_key

    @secret_key.setter
    def secret_key(self, secret_key):
        self._secret_key = str(secret_key)

    ###########################################################################
    @property
    def ec2_connection(self):
        if not self._ec2_connection:
            conn = EC2Connection(self.access_key, self.secret_key)
            self._ec2_connection = conn

        return self._ec2_connection

    ###########################################################################
    def get_all_volumes(self):
        return self.ec2_connection.get_all_volumes()

    ###########################################################################
    def get_all_snapshots(self):
        pass

    ###########################################################################
    def get_snapshot(self, id):
        pass

    ###########################################################################
    def remove_snapshot(self, id):
        pass

    ###########################################################################
    def to_document(self):
        return {
            "_type": "EbsSnapshotTarget",
            "accessKey": self.access_key,
            "secretKey": self.secret_key
        }
    ###########################################################################
    def validate(self):
        errors = []

        if not self.access_key:
            errors.append("Missing 'accessKey' property")

        if not self.secret_key:
            errors.append("Missing 'secretKey' property")

        return errors

###############################################################################
# Target Reference Classes
###############################################################################

class TargetReference(object):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def to_document(self):
        pass

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())

###############################################################################
# FileReference
###############################################################################
class FileReference(TargetReference):

    ###########################################################################
    def __init__(self, file_name=None):
        self.file_name = f

    ###########################################################################
    @property
    def file_name(self):
        return self._file_name


    @file_name.setter
    def file_name(self, file_name):
        self._file_name = file_name

    ###########################################################################
    def to_document(self):
        return {
            "_type": "FileReference",
            "fileName": self.file_name
        }

###############################################################################
# EbsSnapshotReference
###############################################################################
class EbsSnapshotReference(TargetReference):

    ###########################################################################
    def __init__(self, snapshot_id):
        self._snapshot_id = snapshot_id

    ###########################################################################
    @property
    def snapshot_id(self):
        return self._snapshot_id

    @snapshot_id.setter
    def snapshot_id(self, snapshot_id):
        self._snapshot_id = snapshot_id

    ###########################################################################
    def to_document(self):
        return {
            "_type": "EbsSnapshotReference",
            "snapshotId": self.snapshot_id
        }