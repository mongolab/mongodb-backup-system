__author__ = 'abdul'

import traceback
import os
import sys
import shutil
import cloudfiles

import mbs_logging
from base import MBSObject
from utils import which, execute_command
from azure.storage import BlobService
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
MULTIPART_MIN_SIZE = 100 * 1024 * 1024
CF_MULTIPART_MIN_SIZE = 5 * 1024 * 1024 * 1024
MAX_SPLIT_SIZE = 1024 * 1024 * 1024

###############################################################################
# Target Classes
###############################################################################
class BackupTarget(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def put_file(self, file_path, destination_path=None):
        """
            Uploads the specified file path under destination_path.
             destination_path defaults to base name (file name) of file_path
        """
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
def _raise_if_not_connectivity(exception):
    msg = str(exception)
    if ("Broken pipe" in msg or
        "reset" in msg or
        "timed out" in msg):
        logger.warn("Caught a target connectivity exception: %s" % msg)
    else:
        logger.debug("Re-raising a target NON-connectivity exception: %s" %
                     msg)
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
               do_on_exception=_raise_if_not_connectivity)
    def put_file(self, file_path, destination_path=None):
        try:

            # calculating file size
            file_size = os.path.getsize(file_path)
            file_size_in_gb = float(file_size) / (1024 * 1024 * 1024)
            file_size_in_gb = round(file_size_in_gb, 2)

            logger.info("S3BucketTarget: Uploading '%s' (%s GB) to '%s' in "
                        " s3 bucket %s" % (file_path, destination_path,
                                           file_size_in_gb, self.bucket_name))

            destination_path = destination_path or os.path.basename(file_path)

            if file_size >= MULTIPART_MIN_SIZE:
                self._multi_part_put(file_path, destination_path, file_size)
            else:
                self._single_part_put(file_path, destination_path)

            # validate that the file has been uploaded successfully
            self._verify_file_uploaded(destination_path, file_size)

            logger.info("S3BucketTarget: Uploading %s (%s GB) to s3 bucket %s "
                        "completed successfully!!" %
                        (file_path, file_size_in_gb, self.bucket_name))

            return FileReference(file_path=destination_path,
                                 file_size_in_gb=file_size_in_gb)

        except Exception, e:
            traceback.print_exc()
            msg = ("S3BucketTarget: Error while trying to upload '%s'"
                   " to s3 bucket %s. Cause: %s" %
                   (file_path, self.bucket_name, e))
            raise Exception(msg, e)

    ###########################################################################
    def _single_part_put(self, file_path, destination_path):
        bucket = self._get_bucket()
        file_obj = open(file_path)
        k = Key(bucket)
        k.key = destination_path
        k.set_contents_from_file(file_obj)

    ###########################################################################
    def _multi_part_put(self, file_path, destination_path, file_size):
        # create the parts directory, delete/re-create if it already exists
        # for some reason

        try:
            logger.info("S3BucketTarget: Starting multi-part put for %s " %
                        file_path)

            parts_dir = "%s_parts" % file_path
            if os.path.exists(parts_dir):
                shutil.rmtree(parts_dir)

            os.mkdir(parts_dir)
            file_name = os.path.basename(file_path)
            part_prefix = "%s_" % file_name
            # split file into parts
            file_part_paths = self._split_file(file_path, file_size,
                                               parts_dir=parts_dir,
                                               prefix=part_prefix)

            bucket = self._get_bucket()
            mp = bucket.initiate_multipart_upload(destination_path)

            i = 1
            for part_path in file_part_paths:
                part_size = os.path.getsize(part_path)
                fp = open(part_path, 'rb')
                logger.debug("Uploading file part %s %s (%s bytes)" %
                             (i, part_path, part_size))
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
        if chunk_size > MAX_SPLIT_SIZE:
            chunk_size = MAX_SPLIT_SIZE

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

        # IMPORTANT: we have to sort files by name to maintain original order!
        part_paths.sort()

        logger.info("File %s split successfully" % file_path)
        return part_paths

    ###########################################################################
    def _verify_file_uploaded(self, destination_path, file_size):

        bucket = self._get_bucket()
        key = bucket.get_key(destination_path)
        if not key:
            raise Exception("Failure during upload verification: File '%s'"
                            " does not exist in bucket '%s'" %
                            (destination_path, self.bucket_name))
        elif file_size != key.size:
            raise Exception("Failure during upload verification: File size in"
                            " bucket does not match size on disk in bucket "
                            "'%s'" % (destination_path, self.bucket_name))

        # success!

    ###########################################################################
    def get_file(self, file_reference, destination):
        try:

            file_path = file_reference.file_path
            print("Downloading '%s' from s3 bucket '%s'" %
                  (file_path, self.bucket_name))

            bucket = self._get_bucket()
            key = bucket.get_key(file_path)

            if not key:
                raise Exception("No such file '%s' in bucket '%s'" %
                                (file_path, self.bucket_name))
            file_obj = open(os.path.join(destination, file_path), mode="w")

            num_call_backs = key.size / 1000
            key.get_contents_to_file(file_obj, cb=_download_progress,
                                     num_cb=num_call_backs)

            print("Download completed successfully!!")

        except Exception, e:
            msg = ("S3BucketTarget: Error while trying to download '%s'"
                   " from s3 bucket %s. Cause: %s" %
                   (file_path, self.bucket_name, e))
            raise Exception(msg, e)

    ###########################################################################
    def delete_file(self, file_reference):
        try:

            file_path = file_reference.file_path
            logger.info("S3BucketTarget: Deleting '%s' from s3 bucket '%s'" %
                        (file_path, self.bucket_name))

            bucket = self._get_bucket()
            key = bucket.get_key(file_path)
            bucket.delete_key(key)
            logger.info("S3BucketTarget: Successfully deleted '%s' from s3"
                        " bucket '%s'" % (file_path, self.bucket_name))
        except Exception, e:
            msg = ("S3BucketTarget: Error while trying to delete '%s'"
                   " from s3 bucket %s. Cause: %s" %
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
    def put_file(self, file_path, destination_path=None):
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
# RackspaceCloudFilesTarget
###############################################################################
class RackspaceCloudFilesTarget(BackupTarget):

    ###########################################################################
    def __init__(self):
        BackupTarget.__init__(self)
        self._container_name = None
        self._username = None
        self._api_key = None

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=2,
        do_on_exception=_raise_if_not_connectivity)
    def put_file(self, file_path, destination_path=None):
        try:

            # calculating file size
            file_size = os.path.getsize(file_path)
            file_size_in_gb = float(file_size) / (1024 * 1024 * 1024)
            file_size_in_gb = round(file_size_in_gb, 2)
            destination_path = destination_path or os.path.basename(file_path)

            logger.info("RackspaceCloudFilesTarget: Uploading %s (%s GB) "
                        "to container %s" %
                        (file_path, file_size_in_gb, self.container_name))

            if file_size >= CF_MULTIPART_MIN_SIZE:
                self._multi_part_put(file_path, destination_path, file_size)
            else:
                self._single_part_put(file_path, destination_path)

            # validate that the file has been uploaded successfully
            self._verify_file_uploaded(destination_path, file_size)

            logger.info("RackspaceCloudFilesTarget: Uploading %s (%s GB) "
                        "to container %s completed successfully!!" %
                        (file_path, file_size_in_gb, self.container_name))

            return FileReference(file_path=destination_path,
                                 file_size_in_gb=file_size_in_gb)
        except Exception, e:
            traceback.print_exc()
            msg = ("RackspaceCloudFilesTarget: Error while trying to upload "
                   "'%s' to container %s. Cause: %s" %
                   (file_path, self.container_name, e))
            raise Exception(msg, e)

    ###########################################################################
    def _single_part_put(self, file_path, destination_path):
        container = self._get_container()
        container_obj = container.create_object(destination_path)
        container_obj.load_from_filename(file_path)

    ###########################################################################
    def _multi_part_put(self, file_path, destination_path, file_size):
        """
            Uploads file in chunks using Swift Tool (st) command
            http://bazaar.launchpad.net/~hudson-openstack/swift/1.2/view/head:/bin/st

        """
        logger.info("RackspaceCloudFilesTarget: Starting multi-part put "
                    "for %s " % file_path)

        # calculate chunk size
        # split into 10 chunks if possible
        chunk_size = int(file_size / 10)
        if chunk_size > MAX_SPLIT_SIZE:
            chunk_size = MAX_SPLIT_SIZE

        st_exe = which("st")
        st_command = [
            st_exe,
            "-A", "https://auth.api.rackspacecloud.com/v1.0",
            "-U", self.username,
            "-K", self.api_key,
            "upload",
            "--segment-size", str(chunk_size),
            self.container_name, destination_path

        ]
        logger.info("RackspaceCloudFilesTarget: Executing command: %s" %
                    " ".join(st_command))
        working_dir = os.path.dirname(file_path)
        execute_command(st_command, cwd=working_dir)
        logger.info("RackspaceCloudFilesTarget: Multi-part put for %s "
                    "completed successfully!" % file_path)


    ###########################################################################
    def _verify_file_uploaded(self, destination_path, file_size):
        container = self._get_container()
        container_obj = container.get_object(destination_path)
        if not container_obj:
            raise Exception("Failure during upload verification: File '%s'"
                            " does not exist in container '%s'" %
                            (destination_path, self.container_name))
        elif file_size != container_obj.size:
            raise Exception("Failure during upload verification: File size in"
                            " bucket does not match size on disk in bucket "
                            "'%s'" % (destination_path, self.container_name))

            # success!

    ###########################################################################
    def get_file(self, file_reference, destination):
        try:

            file_path = file_reference.file_path
            print("Downloading '%s' from container '%s'" %
                  (file_path, self.container_name))

            container = self._get_container()
            container_obj = container.get_object(file_path)

            if not container_obj:
                raise Exception("No such file '%s' in container '%s'" %
                                (file_path, self.container_name))


            des_file = os.path.join(destination, file_path)
            container_obj.save_to_filename(des_file,
                                           callback=_download_progress)
            print("\nDownload completed successfully!!")

        except Exception, e:
            msg = ("RackspaceCloudFilesTarget: Error while trying to download "
                   "'%s' from container %s. Cause: %s" %
                   (file_path, self.container_name, e))
            raise Exception(msg, e)

    ###########################################################################
    def delete_file(self, file_reference):
        try:

            file_path = file_reference.file_path
            logger.info("RackspaceCloudFilesTarget: Deleting '%s' from "
                        "container '%s'" % (file_path, self.container_name))

            container = self._get_container()
            container.delete_object(file_path)
            logger.info("RackspaceCloudFilesTarget: Successfully deleted '%s' "
                        "from container '%s'" %
                        (file_path, self.container_name))
        except Exception, e:
            msg = ("RackspaceCloudFilesTarget: Error while trying to delete "
                   "'%s' from container %s. Cause: %s" %
                   (file_path, self.container_name, e))
            raise Exception(msg, e)

    ###########################################################################
    @property
    def container_name(self):
        return self._container_name

    @container_name.setter
    def container_name(self, container_name):
        self._container_name = str(container_name)

    ###########################################################################
    def _get_container(self):
        conn = cloudfiles.get_connection(username=self.username,
                                         api_key=self.api_key)

        return conn.get_container(self.container_name)

    ###########################################################################
    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, username):
        self._username = str(username)

    ###########################################################################
    @property
    def api_key(self):
        return self._api_key

    @api_key.setter
    def api_key(self, api_key):
        self._api_key = str(api_key)

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "RackspaceCloudFilesTarget",
            "containerName": self.container_name,
            "username": "xxxxx" if display_only else self.username,
            "apiKey": "xxxxx" if display_only else self.api_key
        }

    ###########################################################################
    def validate(self):
        errors = []

        if not self.container_name:
            errors.append("Missing 'containerName' property")

        if not self.username:
            errors.append("Missing 'username' property")

        if not self.api_key:
            errors.append("Missing 'apiKey' property")

        return errors

###############################################################################
# AzureContainerTarget
###############################################################################
class AzureContainerTarget(BackupTarget):

    ###########################################################################
    def __init__(self):
        BackupTarget.__init__(self)
        self._container_name = None
        self._account_name = None
        self._account_key = None

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=2,
        do_on_exception=_raise_if_not_connectivity)
    def put_file(self, file_path, destination_path):
        try:

            # calculating file size
            file_size = os.path.getsize(file_path)
            file_size_in_gb = float(file_size) / (1024 * 1024 * 1024)
            file_size_in_gb = round(file_size_in_gb, 2)
            destination_path = os.path.basename(file_path)

            logger.info("AzureContainerTarget: Uploading %s (%s GB) "
                        "to container %s" %
                        (file_path, file_size_in_gb, self.container_name))


            self._single_part_put(file_path, destination_path)

            logger.info("AzureContainerTarget: Uploading %s (%s GB) "
                        "to container %s completed successfully!!" %
                        (file_path, file_size_in_gb, self.container_name))

            return FileReference(file_path=destination_path,
                file_size_in_gb=file_size_in_gb)
        except Exception, e:
            traceback.print_exc()
            msg = ("AzureContainerTarget: Error while trying to upload "
                   "'%s' to container %s. Cause: %s" %
                   (file_path, self.container_name, e))
            raise Exception(msg, e)

    ###########################################################################
    def _single_part_put(self, file_path, destination_path):
        blob_service = self._get_blob_service()
        fp = open(file_path, 'r').read()
        blob_service.put_blob(self.container_name, destination_path, fp,
                              x_ms_blob_type='BlockBlob')

    ###########################################################################
    def _multi_part_put(self, file_path, destination_path, file_size):
        pass


    ###########################################################################
    def get_file(self, file_reference, destination):
        raise Exception("AzureContainerTarget: get_file not supported yet")

    ###########################################################################
    def delete_file(self, file_reference):
        raise Exception("AzureContainerTarget: delete_file not supported yet")

    ###########################################################################
    @property
    def container_name(self):
        return self._container_name

    @container_name.setter
    def container_name(self, container_name):
        self._container_name = str(container_name)

    ###########################################################################
    def _get_blob_service(self):
        return BlobService(account_name=self.account_name,
                           account_key=self.account_key,
                           protocol="https")

    ###########################################################################
    @property
    def account_name(self):
        return self._account_name

    @account_name.setter
    def account_name(self, account_name):
        self._account_name = str(account_name)

    ###########################################################################
    @property
    def account_key(self):
        return self._account_key

    @account_key.setter
    def account_key(self, account_key):
        self._account_key = str(account_key)

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "AzureContainerTarget",
            "containerName": self.container_name,
            "accountName": "xxxxx" if display_only else self.account_name,
            "accountKey": "xxxxx" if display_only else self.account_key
        }

    ###########################################################################
    def validate(self):
        errors = []

        if not self.container_name:
            errors.append("Missing 'containerName' property")

        if not self.account_name:
            errors.append("Missing 'accountName' property")

        if not self.account_key:
            errors.append("Missing 'accountKey' property")

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
    def __init__(self, file_path=None, file_size_in_gb=None):
        TargetReference.__init__(self)
        self.file_path = file_path
        self.file_size_in_gb = file_size_in_gb

    ###########################################################################
    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, file_path):
        self._file_path = file_path

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "_type": "FileReference",
            "filePath": self.file_path,
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


###############################################################################
# HELPERS
###############################################################################
def _download_progress(transferred, size):
    percentage = (float(transferred)/float(size)) * 100
    sys.stdout.write("\rDownloaded %s bytes of %s. %%%i "
                     "completed" %
                     (transferred, size, percentage))
    sys.stdout.flush()