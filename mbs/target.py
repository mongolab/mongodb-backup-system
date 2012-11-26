__author__ = 'abdul'

import traceback
import os
import sys
import shutil

import mbs_logging
from base import MBSObject
from utils import which, execute_command

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.ec2 import EC2Connection
from robustify.robustify import robustify

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
# CONSTANTS
###############################################################################
S3_MULTIPART_MIN_SIZE = 100 * 1024 * 1024

S3_MAX_SPLIT_SIZE = 1024 * 1024 * 1024

###############################################################################
# Target Classes
###############################################################################
class BackupTarget(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def put_file(self, file_path):
        pass

    ###########################################################################
    def get_file(self, file_reference, destination):
        """
            Gets the file references and writes it to the specified destination
        """

    ###########################################################################
    def delete_file(self, file_reference):
        """
            Deletes the specified f file reference
        """
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
def _raise_if_not_s3_connectivity(exception):
    msg = str(exception)
    if "Broken pipe" in msg or "reset" in msg:
        logger.warn("Caught an s3 connectivity exception: %s" % msg)
    else:
        logger.debug("Re-raising a an s3 NON-connectivity exception: %s" % msg)
        raise

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
    @robustify(max_attempts=3, retry_interval=2,
               do_on_exception=_raise_if_not_s3_connectivity)
    def put_file(self, file_path):
        try:

            # calculating file size
            file_size = os.path.getsize(file_path)
            file_size_in_gb = file_size / (1024 * 1024 * 1024)
            file_size_in_gb = round(file_size_in_gb, 2)

            logger.info("S3BucketTarget: Uploading %s (%s GB) to s3 bucket %s" %
                        (file_path, file_size_in_gb, self.bucket_name))

            file_key = os.path.basename(file_path)

            if file_size >= S3_MULTIPART_MIN_SIZE:
                self._multi_part_put(file_key, file_path, file_size)
            else:
                self._single_part_put(file_key, file_path)

            logger.info("S3BucketTarget: Uploading %s (%s GB) to s3 bucket %s "
                        "completed successfully!!" %
                        (file_path, file_size_in_gb, self.bucket_name))

            return FileReference(file_name=file_key,
                                 file_size_in_gb=file_size_in_gb)
        except Exception, e:
            traceback.print_exc()
            msg = ("S3BucketTarget: Error while trying to upload '%s'"
                   " to s3 bucket %s. Cause: %s" %
                   (file_path, self.bucket_name, e))
            raise Exception(msg, e)

    ###########################################################################
    def _single_part_put(self, file_key, file_path):
        bucket = self._get_bucket()
        file_obj = open(file_path)
        k = Key(bucket)
        k.key = file_key
        k.set_contents_from_file(file_obj)

    ###########################################################################
    def _multi_part_put(self, file_key, file_path, file_size):
        # create the parts directory, delete/re-create if it already exists
        # for some reason

        try:
            logger.info("S3BucketTarget: Starting multi-part put for %s " %
                        file_path)

            parts_dir = "%s_parts" % file_path
            if os.path.exists(parts_dir):
                shutil.rmtree(parts_dir)

            os.mkdir(parts_dir)
            part_prefix = "%s_" % file_key
            # split file into parts
            file_part_paths = self._split_file(file_path, file_size,
                                               parts_dir=parts_dir,
                                               prefix=part_prefix)

            bucket = self._get_bucket()
            mp = bucket.initiate_multipart_upload(file_key)

            i = 1
            for part_path in file_part_paths:
                fp = open(part_path, 'rb')
                mp.upload_part_from_file(fp, i)
                fp.close()
                i += 1

            mp.complete_upload()
            logger.info("S3BucketTarget: Multi-part put for %s completed"
                        " successfully!" % file_path)
        finally:
            logger.info("S3BucketTarget: Cleaning multi-part temp "
                        "folders/files")
            # cleanup
            if os.path.exists(parts_dir):
                shutil.rmtree(parts_dir)

    ###########################################################################
    def _split_file(self, file_path, file_size,
                    parts_dir=None, prefix=None):
        """
            Splits the specified file into 10 parts (if each part is less than
             max size otherwise file_size/max_size)
             Returns list of file part paths
        """
        logger.info("Splitting file '%s' into multiple parts" % file_path)

        split_exe = which("split")
        # split into 10 chunks if possible
        chunk_size = int(file_size / 10)
        if chunk_size > S3_MAX_SPLIT_SIZE:
            chunk_size = S3_MAX_SPLIT_SIZE

        dest = os.path.join(parts_dir, prefix)
        split_cmd = [split_exe, "-b", str(chunk_size), file_path, dest]
        cmd_display = " ".join(split_cmd)

        logger.info("Running split command: %s" % cmd_display)
        execute_command(split_cmd)

        # construct return value
        part_names = os.listdir(parts_dir)
        part_paths = []
        for name in part_names:
            part_path = os.path.join(parts_dir, name)
            part_paths.append(part_path)

        logger.info("File %s split successfully" % file_path)
        return part_paths

    ###########################################################################
    def get_file(self, file_reference, destination):
        try:

            file_name = file_reference.file_name
            print("Downloading '%s' from s3 bucket '%s'" %
                        (file_name, self.bucket_name))

            bucket = self._get_bucket()
            key = bucket.get_key(file_name)

            if not key:
                raise Exception("No such file '%s' in bucket '%s'" %
                                (file_name, self.bucket_name))
            file_obj = open(os.path.join(destination, file_name), mode="w")

            def download_progress(bytes_downloaded, total):
                percentage = (float(bytes_downloaded)/float(total)) * 100
                sys.stdout.write("\rDownloaded %s bytes of %s. %%%i completed" %
                                 (bytes_downloaded, total, percentage))
                sys.stdout.flush()

            num_call_backs = key.size / 1000
            key.get_contents_to_file(file_obj, cb=download_progress,
                                     num_cb=num_call_backs)

            print("Download completed successfully!!")

        except Exception, e:
            msg = ("S3BucketTarget: Error while trying to download '%s'"
                   " from s3 bucket %s. Cause: %s" %
                   (file_name, self.bucket_name, e))
            raise Exception(msg, e)

    ###########################################################################
    def delete_file(self, file_reference):
        try:

            file_name = file_reference.file_name
            logger.info("S3BucketTarget: Deleting '%s' from s3 bucket '%s'" %
                        (file_name, self.bucket_name))

            bucket = self._get_bucket()
            key = bucket.get_key(file_name)
            bucket.delete_key(key)
            logger.info("S3BucketTarget: Successfully deleted'%s' from s3"
                        " bucket '%s'" % (file_name, self.bucket_name))
        except Exception, e:
            msg = ("S3BucketTarget: Error while trying to delete '%s'"
                   " from s3 bucket %s. Cause: %s" %
                   (file_name, self.bucket_name, e))
            raise Exception(msg, e)

    ###########################################################################
    @property
    def bucket_name(self):
        return self._bucket_name

    @bucket_name.setter
    def bucket_name(self, bucket_name):
        self._bucket_name = str(bucket_name)

    ###########################################################################
    def _get_bucket(self):
        conn = S3Connection(self.access_key, self.secret_key)
        return conn.get_bucket(self.bucket_name)

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
    def to_document(self, display_only=False):
        return {
            "_type": "S3BucketTarget",
            "bucketName": self.bucket_name,
            "accessKey": "xxxxx" if display_only else self.access_key,
            "secretKey": "xxxxx" if display_only else self.secret_key
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
    def to_document(self, display_only=False):
        return {
            "_type": "EbsSnapshotTarget",
            "accessKey": "xxxxx" if display_only else self.access_key,
            "secretKey": "xxxxx" if display_only else self.secret_key
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

class TargetReference(MBSObject):
    """
        Represents a reference to the file that gets uploaded to target
    """
    ###########################################################################
    def __init__(self):
        self._expired = False
        self._file_size_in_gb = None

    ###########################################################################
    @property
    def expired(self):
        """
            Indicates if the reference file expired.

        """
        return self._expired

    @expired.setter
    def expired(self, expired):
        self._expired = expired

    ###########################################################################
    @property
    def file_size_in_gb(self):
        """
            Indicates if the reference file expired.

        """
        return self._file_size_in_gb

    @file_size_in_gb.setter
    def file_size_in_gb(self, size):
        self._file_size_in_gb = size

###############################################################################
# FileReference
###############################################################################
class FileReference(TargetReference):

    ###########################################################################
    def __init__(self, file_name=None, file_size_in_gb=None):
        TargetReference.__init__(self)
        self.file_name = file_name
        self.file_size_in_gb = file_size_in_gb

    ###########################################################################
    @property
    def file_name(self):
        return self._file_name

    @file_name.setter
    def file_name(self, file_name):
        self._file_name = file_name

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "_type": "FileReference",
            "fileName": self.file_name,
            "fileSizeInGB": self.file_size_in_gb
        }
        if self.expired:
            doc["expired"] = self.expired
        return doc

###############################################################################
# EbsSnapshotReference
###############################################################################
class EbsSnapshotReference(TargetReference):

    ###########################################################################
    def __init__(self, snapshot_id):
        TargetReference.__init__(self)
        self._snapshot_id = snapshot_id

    ###########################################################################
    @property
    def snapshot_id(self):
        return self._snapshot_id

    @snapshot_id.setter
    def snapshot_id(self, snapshot_id):
        self._snapshot_id = snapshot_id

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "_type": "EbsSnapshotReference",
            "snapshotId": self.snapshot_id,
            "expired": self.expired
        }
        if self.expired:
            doc["expired"] = self.expired
        return doc