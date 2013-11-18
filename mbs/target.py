__author__ = 'abdul'


import os
import sys

import cloudfiles
import cloudfiles.errors

import mbs_logging

from mbs import get_mbs
from base import MBSObject
from utils import which, execute_command
from azure.storage import BlobService
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.ec2 import EC2Connection
from errors import *
from robustify.robustify import robustify
from splitfile import SplitFile
from threading import Thread

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

# Cloud block storage statuses
CBS_STATUS_PENDING = "pending"
CBS_STATUS_COMPLETED = "completed"
CBS_STATUS_ERROR = "error"

###############################################################################
# Target Classes
###############################################################################
class BackupTarget(MBSObject):

    ###########################################################################
    def __init__(self):
        self._preserve = None

    ###########################################################################
    @property
    def container_name(self):
        """
            Should be implemented by subclasses
        """

    ###########################################################################
    @property
    def target_type(self):
        """
            returns the target type which is the class name
        """
        return self.__class__.__name__

    ###########################################################################
    def put_file(self, file_path, destination_path=None,
                 overwrite_existing=False):
        """
            Uploads the specified file path under destination_path.
             destination_path defaults to base name (file name) of file_path
             This is the generic implementation that includes upload
             verification and returning proper errors
        """
        try:

            destination_path = destination_path or os.path.basename(file_path)
            # calculating file size
            file_size = os.path.getsize(file_path)
            logger.info("%s: Uploading '%s' (%s bytes) to '%s' in "
                        " container %s" % (self.target_type, file_path,
                                           file_size, destination_path,
                                           self.container_name))


            if not overwrite_existing:
                logger.info("%s: Verifying file '%s' does not exist in "
                            "container '%s' before attempting to upload"%
                            (self.target_type, destination_path,
                             self.container_name))
                if self.file_exists(destination_path):
                    msg = ("File '%s' already exists in container '%s'" %
                           (destination_path, self.container_name))
                    raise UploadedFileAlreadyExistError(msg)

            target_ref = self.do_put_file(file_path,
                                          destination_path=destination_path)
            # set the preserve field
            target_ref.preserve = self.preserve

            # validate that the file has been uploaded successfully
            self._verify_file_uploaded(destination_path, file_size)

            logger.info("%s: Uploading %s (%s bytes) to container %s "
                        "completed successfully!!" %
                        (self.target_type, file_path, file_size,
                         self.container_name))

            return target_ref
        except Exception, e:
            logger.exception("BackupTarget.put_file(): Exception caught ")
            if isinstance(e, TargetError):
                raise
            elif is_connection_exception(e):
                raise TargetConnectionError(self.container_name, cause=e)
            else:
                raise TargetUploadError(destination_path, self.container_name,
                                        cause=e)

    ###########################################################################
    def do_put_file(self, file_path, destination_path=None):
        """
           does the actually work. should be implemented by subclasses
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
            Generic implementation of deleting a file reference by delegating
            to abstract do_delete_file and decorating it with proper validation
            and errors
        """
        return self._robustified_delete_file(file_reference)

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=5,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def _robustified_delete_file(self, file_reference):
        file_exists = self.do_delete_file(file_reference)
        if not file_exists:
            msg = ("Attempted to delete a file ('%s') that does not exist in"
                   " container '%s'" % (file_reference.file_path,
                                        self.container_name))
            logger.warning(msg)

        self._verify_file_deleted(file_reference.file_path)
        return file_exists

    ###########################################################################
    def do_delete_file(self, file_reference):
        """
            Should be overridden by subclasses
            Returns a boolean indicating if file did exist before deleting it
        """
        return False

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

    ###########################################################################
    @robustify(max_attempts=10, retry_interval=5,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def _verify_file_uploaded(self, destination_path, file_size):

        dest_exists, dest_size = self._fetch_file_info(destination_path)
        cname = self.container_name

        if not dest_exists:
            raise UploadedFileDoesNotExistError(destination_path, cname)
        elif file_size != dest_size:
            raise UploadedFileSizeMatchError(destination_path, cname,
                                             dest_size, file_size)

    ###########################################################################
    def _verify_file_deleted(self, file_path):

        file_exists, file_size = self._fetch_file_info(file_path)
        if file_exists:
            msg = ("%s: Failure during delete verification: File '%s' still"
                   " exists in container '%s'" %
                   (self.target_type, file_path, self.container_name))
            raise TargetDeleteError(msg)

    ###########################################################################
    def file_exists(self, file_path):

        file_exists, file_size = self._fetch_file_info(file_path)
        return file_exists

    ###########################################################################
    def _fetch_file_info(self, destination_path):
        """
            Returns a tuple of (file_exists, file_size)
            Should be implemented by subclasses
        """
    ###########################################################################
    @property
    def preserve(self):
        return self._preserve

    @preserve.setter
    def preserve(self, val):
        self._preserve = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {

        }

        if self.preserve is not None:
            doc["preserve"] = self.preserve

        return doc

###############################################################################
# S3BucketTarget
###############################################################################
class S3BucketTarget(BackupTarget):

    ###########################################################################
    def __init__(self):
        BackupTarget.__init__(self)
        self._bucket_name = None
        self._encrypted_access_key = None
        self._encrypted_secret_key = None

    ###########################################################################
    def do_put_file(self, file_path, destination_path=None):

        # determine single/multi part upload
        file_size = os.path.getsize(file_path)

        if file_size >= MULTIPART_MIN_SIZE:
            self._multi_part_put(file_path, destination_path, file_size)
        else:
            self._single_part_put(file_path, destination_path)

        return FileReference(file_path=destination_path,
                             file_size=file_size)

    ###########################################################################
    def _fetch_file_info(self, destination_path):
        """
            Override by s3 specifics

        """
        bucket = self._get_bucket()
        key = bucket.get_key(destination_path)
        if key:
            return True, key.size
        else:
            return False, None

    ###########################################################################
    def _single_part_put(self, file_path, destination_path):
        bucket = self._get_bucket()
        file_obj = open(file_path)
        k = Key(bucket)
        k.key = destination_path
        k.set_contents_from_file(file_obj)

    ###########################################################################
    def _multi_part_put(self, file_path, destination_path, file_size):

        logger.info("S3BucketTarget: Starting multi-part put for %s " %
                    file_path)
        chunk_size = int(file_size / 10)
        if chunk_size > MAX_SPLIT_SIZE:
            chunk_size = MAX_SPLIT_SIZE

        bucket = self._get_bucket()
        mp = bucket.initiate_multipart_upload(destination_path)

        upload = SplitFile(file_path, chunk_size)

        for i, chunk in enumerate(upload, 1):
            logger.debug("Uploading file part %d (%s bytes)" %
                         (i, chunk.size))
            mp.upload_part_from_file(chunk, i)

        mp.complete_upload()
        logger.info("S3BucketTarget: Multi-part put for %s completed"
                    " successfully!" % file_path)

    ###########################################################################
    def get_file(self, file_reference, destination):
        try:

            file_path = file_reference.file_path
            file_name = file_reference.file_name

            print("Downloading '%s' from s3 bucket '%s'" %
                  (file_path, self.bucket_name))

            bucket = self._get_bucket()
            key = bucket.get_key(file_path)

            if not key:
                raise TargetFileNotFoundError("No such file '%s' in bucket "
                                              "'%s'" % (file_path,
                                                        self.bucket_name))

            file_obj = open(os.path.join(destination, file_name), mode="w")

            num_call_backs = key.size / 1000
            key.get_contents_to_file(file_obj, cb=_download_progress,
                                     num_cb=num_call_backs)

            print("Download completed successfully!!")

        except Exception, e:
            msg = ("S3BucketTarget: Error while trying to download '%s'"
                   " from s3 bucket %s. Cause: %s" %
                   (file_path, self.bucket_name, e))
            raise TargetError(msg, cause=e)

    ###########################################################################
    def do_delete_file(self, file_reference):
        try:

            file_path = file_reference.file_path
            logger.info("S3BucketTarget: Deleting '%s' from s3 bucket '%s'" %
                        (file_path, self.bucket_name))

            bucket = self._get_bucket()
            key = bucket.get_key(file_path)
            if not key:
                return False

            bucket.delete_key(key)
            logger.info("S3BucketTarget: Successfully deleted '%s' from s3"
                        " bucket '%s'" % (file_path, self.bucket_name))
            return True
        except Exception, e:
            msg = ("S3BucketTarget: Error while trying to delete '%s'"
                   " from s3 bucket %s. Cause: %s" %
                   (file_path, self.bucket_name, e))
            raise TargetDeleteError(msg, cause=e)

    ###########################################################################
    @property
    def container_name(self):
        return self.bucket_name

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
        if self.encrypted_access_key:
            return get_mbs().encryptor.decrypt_string(self.encrypted_access_key)

    @access_key.setter
    def access_key(self, access_key):
        if access_key:
            eak = get_mbs().encryptor.encrypt_string(str(access_key))
            self.encrypted_access_key = eak

    ###########################################################################
    @property
    def secret_key(self):
        if self.encrypted_secret_key:
            return get_mbs().encryptor.decrypt_string(self.encrypted_secret_key)

    @secret_key.setter
    def secret_key(self, secret_key):
        if secret_key:
            sak = get_mbs().encryptor.encrypt_string(str(secret_key))
            self.encrypted_secret_key = sak

    ###########################################################################
    @property
    def encrypted_access_key(self):
        return self._encrypted_access_key

    @encrypted_access_key.setter
    def encrypted_access_key(self, val):
        if val:
            self._encrypted_access_key = val.encode('ascii', 'ignore')

    ###########################################################################
    @property
    def encrypted_secret_key(self):
        return self._encrypted_secret_key

    @encrypted_secret_key.setter
    def encrypted_secret_key(self, val):
        if val:
            self._encrypted_secret_key = val.encode('ascii', 'ignore')

    ###########################################################################
    def to_document(self, display_only=False):
        ak = "xxxxx" if display_only else self.encrypted_access_key
        sk = "xxxxx" if display_only else self.encrypted_secret_key

        doc = BackupTarget.to_document(self, display_only=display_only)
        doc.update({
            "_type": "S3BucketTarget",
            "bucketName": self.bucket_name,
            "encryptedAccessKey": ak,
            "encryptedSecretKey": sk
        })

        return doc

    ###########################################################################
    def validate(self):
        errors = []

        if not self.bucket_name:
            errors.append("Missing 'bucketName' property")

        if not self.access_key:
            errors.append("Missing 'encryptedAccessKey' property")

        if not self.secret_key:
            errors.append("Missing 'encryptedSecretKey' property")

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
        doc = BackupTarget.to_document(self, display_only=display_only)
        doc.update({
            "_type": "EbsSnapshotTarget",
            "accessKey": "xxxxx" if display_only else self.access_key,
            "secretKey": "xxxxx" if display_only else self.secret_key
        })

        return doc

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
        self._encrypted_username = None
        self._encrypted_api_key = None

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=5,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def do_put_file(self, file_path, destination_path=None):

        # determine single/multi part upload
        file_size = os.path.getsize(file_path)

        destination_path = destination_path or os.path.basename(file_path)


        if file_size >= CF_MULTIPART_MIN_SIZE:
            self._multi_part_put(file_path, destination_path, file_size)
        else:
            self._single_part_put(file_path, destination_path)

        return FileReference(file_path=destination_path,
                             file_size=file_size)

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
    def _fetch_file_info(self, destination_path):
        container = self._get_container()
        try:
            container_obj = container.get_object(destination_path)
            if container_obj:
                return True, container_obj.size

        except cloudfiles.errors.NoSuchObject:
            pass

        return False, None

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


            file_name = file_reference.file_name
            des_file = os.path.join(destination, file_name)
            container_obj.save_to_filename(des_file,
                                           callback=_download_progress)
            print("\nDownload completed successfully!!")

        except Exception, e:
            msg = ("RackspaceCloudFilesTarget: Error while trying to download "
                   "'%s' from container %s. Cause: %s" %
                   (file_path, self.container_name, e))
            raise TargetError(msg, e)

    ###########################################################################
    def do_delete_file(self, file_reference):
        try:

            file_path = file_reference.file_path
            logger.info("RackspaceCloudFilesTarget: Deleting '%s' from "
                        "container '%s'" % (file_path, self.container_name))

            container = self._get_container()
            container.delete_object(file_path)
            logger.info("RackspaceCloudFilesTarget: Successfully deleted '%s' "
                        "from container '%s'" %
                        (file_path, self.container_name))
            return True
        except Exception, e:
            # handle case when file does not exist
            err = str(e)
            if "404" in err:
                return False

            msg = ("RackspaceCloudFilesTarget: Error while trying to delete "
                   "'%s' from container %s. Cause: %s" %
                   (file_path, self.container_name, e))
            raise TargetDeleteError(msg, e)

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
                                         api_key=self.api_key,
                                         timeout=30)

        return conn.get_container(self.container_name)

    ###########################################################################
    @property
    def username(self):
        if self.encrypted_username:
            return get_mbs().encryptor.decrypt_string(self.encrypted_username)

    @username.setter
    def username(self, username):
        if username:
            eu = get_mbs().encryptor.encrypt_string(str(username))
            self.encrypted_username = eu

    ###########################################################################
    @property
    def api_key(self):
        if self.encrypted_api_key:
            return get_mbs().encryptor.decrypt_string(self.encrypted_api_key)

    @api_key.setter
    def api_key(self, api_key):
        if api_key:
            eak = get_mbs().encryptor.encrypt_string(str(api_key))
            self.encrypted_api_key = eak

    ###########################################################################
    @property
    def encrypted_username(self):
        return self._encrypted_username

    @encrypted_username.setter
    def encrypted_username(self, value):
        if value:
            self._encrypted_username = value.encode('ascii', 'ignore')

    ###########################################################################
    @property
    def encrypted_api_key(self):
        return self._encrypted_api_key

    @encrypted_api_key.setter
    def encrypted_api_key(self, value):
        if value:
            self._encrypted_api_key = value.encode('ascii', 'ignore')

    ###########################################################################
    def to_document(self, display_only=False):

        doc = BackupTarget.to_document(self, display_only=display_only)

        eu = "xxxxx" if display_only else self.encrypted_username
        eak = "xxxxx" if display_only else self.encrypted_api_key
        doc.update({
            "_type": "RackspaceCloudFilesTarget",
            "containerName": self.container_name,
            "encryptedUsername": eu,
            "encryptedApiKey": eak
        })

        return doc

    ###########################################################################
    def validate(self):
        errors = []

        if not self.container_name:
            errors.append("Missing 'containerName' property")

        if not self.username:
            errors.append("Missing 'encryptedUsername' property")

        if not self.api_key:
            errors.append("Missing 'encryptedApiKey' property")

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
    def put_file(self, file_path, destination_path):
        try:

            # calculating file size
            file_size = os.path.getsize(file_path)
            destination_path = os.path.basename(file_path)

            logger.info("AzureContainerTarget: Uploading %s (%s bytes) "
                        "to container %s" %
                        (file_path, file_size, self.container_name))


            self._single_part_put(file_path, destination_path)

            logger.info("AzureContainerTarget: Uploading %s (%s bytes) "
                        "to container %s completed successfully!!" %
                        (file_path, file_size, self.container_name))

            return FileReference(file_path=destination_path,
                                 file_size=file_size)
        except Exception, e:
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
        doc = BackupTarget.to_document(self, display_only=display_only)
        doc.update({
            "_type": "AzureContainerTarget",
            "containerName": self.container_name,
            "accountName": "xxxxx" if display_only else self.account_name,
            "accountKey": "xxxxx" if display_only else self.account_key
        })

        return doc

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
    def __init__(self, preserve=None):
        self._file_size = None
        self._preserve = preserve
        self._deleted_date = None

    ###########################################################################
    @property
    def preserve(self):
        return self._preserve

    @preserve.setter
    def preserve(self, val):
        self._preserve = val

    ###########################################################################
    @property
    def deleted_date(self):
        return self._deleted_date

    @deleted_date.setter
    def deleted_date(self, val):
        self._deleted_date = val

    ###########################################################################
    @property
    def deleted(self):
        return self.deleted_date is not None

    ###########################################################################
    @property
    def file_size(self):
        return self._file_size

    @file_size.setter
    def file_size(self, file_size):
        self._file_size = file_size

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {

        }

        if self.preserve is not None:
            doc["preserve"] = self.preserve

        if self.deleted_date:
            doc["deletedDate"] = self.deleted_date

        return doc

###############################################################################
# FileReference
###############################################################################
class FileReference(TargetReference):

    ###########################################################################
    def __init__(self, file_path=None, file_size=None, preserve=None):
        TargetReference.__init__(self, preserve=preserve)
        self.file_path = file_path
        self.file_size = file_size

    ###########################################################################
    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, file_path):
        self._file_path = file_path

    ###########################################################################
    @property
    def file_name(self):
        return os.path.basename(self.file_path)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = TargetReference.to_document(self, display_only=display_only)
        doc.update({
            "_type": "FileReference",
            "filePath": self.file_path,
            "fileSize": self.file_size
        })

        return doc

###############################################################################
# CloudBlockStorageSnapshotReference
###############################################################################
class CloudBlockStorageSnapshotReference(TargetReference):
    """
        Base class for cloud block storage snapshot references
    """
    ###########################################################################
    def __init__(self, cloud_block_storage=None, status=None):
        TargetReference.__init__(self)
        self._cloud_block_storage = cloud_block_storage
        self._status = status

    ###########################################################################
    @property
    def cloud_block_storage(self):
        return self._cloud_block_storage

    @cloud_block_storage.setter
    def cloud_block_storage(self, val):
        self._cloud_block_storage = val

    ###########################################################################
    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status):
        self._status = status

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "cloudBlockStorage":
                self.cloud_block_storage.to_document(display_only=display_only),
            "status": self.status
        }

###############################################################################
# EbsSnapshotReference
###############################################################################
class EbsSnapshotReference(CloudBlockStorageSnapshotReference):

    ###########################################################################
    def __init__(self, snapshot_id=None, cloud_block_storage=None, status=None,
                 volume_size=None, progress=None, start_time=None ):
        CloudBlockStorageSnapshotReference.__init__(self, cloud_block_storage=
                                                           cloud_block_storage,
                                                          status=status)
        self._snapshot_id = snapshot_id
        self._volume_size = volume_size
        self._progress = progress
        self._start_time = start_time

    ###########################################################################
    @property
    def snapshot_id(self):
        return self._snapshot_id

    @snapshot_id.setter
    def snapshot_id(self, snapshot_id):
        self._snapshot_id = snapshot_id

    ###########################################################################
    @property
    def volume_size(self):
        return self._volume_size

    @volume_size.setter
    def volume_size(self, volume_size):
        self._volume_size = volume_size

    ###########################################################################
    @property
    def progress(self):
        return self._progress

    @progress.setter
    def progress(self, progress):
        self._progress = progress

    ###########################################################################
    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time

    ###########################################################################
    def to_document(self, display_only=False):
        doc = CloudBlockStorageSnapshotReference.to_document(self,display_only=
                                                                  display_only)
        doc.update({
            "_type": "EbsSnapshotReference",
            "snapshotId": self.snapshot_id,
            "volumeSize": self.volume_size,
            "progress": self.progress,
            "startTime": self.start_time
        })

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



###############################################################################
# Concurrent multi target upload
###############################################################################
def multi_target_upload_file(targets,
                             file_path, destination_path=None,
                             overwrite_existing=False):

    logger.info("MULTI TARGET UPLOAD: Starting concurrent target upload for "
                "file '%s'" % file_path)
    uploaders = []

    # first kick off the uploads
    for target in targets:
        target_uploader = TargetUploader(target,
                                         file_path,
                                         destination_path=destination_path,
                                         overwrite_existing=overwrite_existing)
        uploaders.append(target_uploader)
        logger.info("Starting uploader for target: %s" % target)
        target_uploader.start()

    logger.info("Waiting for all target uploaders to finish")
    # wait for all target uploaders to finish
    for target_uploader in uploaders:
        logger.info("Waiting for target uploader for to "
                    "finish: %s" % target_uploader.target)
        target_uploader.join()
        if target_uploader.error:
            logger.info("Target uploader %s for %s to "
                        "finished with an error." %
                        (target_uploader.target, file_path))
        else:
            logger.info("Target uploader %s for %s to "
                        "finished successfully! Target ref: %s" %
                        (file_path, target_uploader.target,
                         target_uploader.target_reference))

    logger.info("MULTI TARGET UPLOAD: SUCCESSFULLY uploaded '%s'!" % file_path)

    return uploaders

###############################################################################
# TargetUploader class
###############################################################################
class TargetUploader(Thread):
###############################################################################
    def __init__(self, target, file_path, **upload_kargs):
        Thread.__init__(self)
        self._target = target
        self._upload_kargs = upload_kargs
        self._target_reference = None
        self._file_path = file_path
        self._error = None

    ###########################################################################
    def run(self):
        try:
            tr = self._target.put_file(self._file_path,
                                       **self._upload_kargs)
            self._target_reference = tr
        except Exception, ex:
            self._error = ex

    ###########################################################################
    @property
    def target(self):
        return self._target

    ###########################################################################
    @property
    def target_reference(self):
        return self._target_reference

    ###########################################################################
    @property
    def error(self):
        return self._error

    ###########################################################################
    def completed(self):
        return self.target_reference is not None or self.error is not None