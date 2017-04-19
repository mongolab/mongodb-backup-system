__author__ = 'abdul'


import os
import sys
import logging
import uuid
import re

import cloudfiles
import cloudfiles.errors

import cloudfiles_utils
import mbs
import s3_utils

from base import MBSObject
from utils import which, execute_command, export_mbs_object_list, safe_stringify
from azure.storage.blob.baseblobservice import BaseBlobService
from boto.s3.key import Key
from boto.exception import S3ResponseError
from cloudfiles.errors import NoSuchContainer, AuthenticationFailed

import errors
from robustify.robustify import robustify
from splitfile import SplitFile
from threading import Thread
import requests

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

###############################################################################
# CONSTANTS
###############################################################################
MULTIPART_MIN_SIZE = 100 * 1024 * 1024
CF_MULTIPART_MIN_SIZE = 5 * 1024 * 1024 * 1024
MAX_SPLIT_SIZE = 1024 * 1024 * 1024


# Cloud block storage statuses
class SnapshotStatus(object):
    PENDING = "pending"
    COMPLETED = "completed"
    ERROR = "error"

###############################################################################
# Target Classes
###############################################################################
class BackupTarget(MBSObject):

    ###########################################################################
    def __init__(self):
        MBSObject.__init__(self)
        self._preserve = None
        self._credentials = None
        self._cloud_storage_encryption_enabled = False
        self._tags = None

    ###########################################################################
    @property
    def container_name(self):
        """
            Should be implemented by subclasses
        """

    ###########################################################################
    @property
    def credentials(self):
        return self._credentials

    ###########################################################################
    @credentials.setter
    def credentials(self, val):
        self._credentials = val

    ###########################################################################
    @property
    def target_type(self):
        """
            returns the target type which is the class name
        """
        return self.__class__.__name__

    ###########################################################################
    @property
    def cloud_storage_encryption_enabled(self):
        return self._cloud_storage_encryption_enabled

    @cloud_storage_encryption_enabled.setter
    def cloud_storage_encryption_enabled(self, val):
        """
        TODO: XXX when cloudStorageEncryptionEnabled references are removed from existing document
        :param val:
        :return:
        """
        self._cloud_storage_encryption_enabled = bool(val)

    ###########################################################################
    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        self._tags = tags

    ###########################################################################
    def put_file(self, file_path, destination_path=None,
                 overwrite_existing=True, metadata=None):
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
                    raise errors.UploadedFileAlreadyExistError(msg)

            target_ref = self._robustifiled_put_file(
                file_path,
                destination_path=destination_path,
                metadata=metadata)
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
            if isinstance(e, errors.TargetError):
                raise
            elif errors.is_connection_exception(e):
                raise errors.TargetConnectionError(self.container_name, cause=e)
            else:
                raise errors.TargetUploadError(destination_path, self.container_name,
                                        cause=e)

    ###########################################################################
    def _robustifiled_put_file(self, file_path, destination_path,
                               metadata=None):
        attempt_counter = {
            "count": 0
        }
        return self._do_robustifiled_put_file(
            attempt_counter, file_path,
            destination_path,
            metadata=metadata)

    ###########################################################################
    @robustify(max_attempts=10, retry_interval=5,
               backoff=2,
               do_on_exception=errors.raise_if_not_retriable,
               do_on_failure=errors.raise_exception,)
    def _do_robustifiled_put_file(self, attempt_counter,
                                  file_path, destination_path,
                                  metadata=None):
        """
           a robustified put file
        """
        attempt_counter["count"] += 1
        logger.debug("_robustifiled_put_file(): Attempting to upload file '%s'"
                     " to container '%s' (attempt # %s)" %
                     (file_path, self.container_name,
                      attempt_counter["count"]))
        # check if we don't need to reupload the file if it was already
        # uploaded through a previous attempt but got interrupted (like
        # connection reset etc)
        if attempt_counter["count"] > 1:
            file_size = os.path.getsize(file_path)
            if self.file_exists(destination_path,
                                expected_file_size=file_size):
                logger.debug("File uploaded through a previous attempt! "
                             "nothing to do!")
                return FileReference(file_path=destination_path,
                                     file_size=file_size)

        return self.do_put_file(file_path, destination_path,
                                metadata=metadata)

    ###########################################################################
    def do_put_file(self, file_path, destination_path, metadata=None):
        """
           does the actual work. should be implemented by subclasses
        """
        pass

    ###########################################################################
    def get_file(self, file_reference, destination):
        """
            Gets the file references and writes it to the specified destination
        """

    ###########################################################################
    def get_temp_download_url(self, file_reference):
        """
            returns a presigned url to download specified reference
        """
        raise Exception( "Not implemented")

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
               do_on_exception=errors.raise_if_not_retriable,
               do_on_failure=errors.raise_exception)
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
    def has_sufficient_permissions(self):
        """
         Returns an array containing error messages (if any). Empty if user has
         sufficient permissions
        """
        return []

    ###########################################################################
    def validate(self):
        """
         Returns an array containing validation messages (if any). Empty if no
         validation errors
        """
        return []

    ###########################################################################
    @robustify(max_attempts=10, retry_interval=5,
               do_on_exception=errors.raise_if_not_retriable,
               do_on_failure=errors.raise_exception)
    def _verify_file_uploaded(self, destination_path, file_size):

        file_info = self._fetch_file_info(destination_path)
        cname = self.container_name

        if not file_info:
            raise errors.UploadedFileDoesNotExistError(destination_path, cname)
        elif self.cloud_storage_encryption_enabled and not file_info.get('cloud_storage_encryption', None):
            raise errors.UploadedFileIsNotEncrypted(destination_path, self.container_name)
        elif file_size != file_info['size']:
            raise errors.UploadedFileSizeMatchError(destination_path, cname,
                                             file_info['size'], file_size)

    ###########################################################################
    def _verify_file_deleted(self, file_path):
        file_info = self._fetch_file_info(file_path)
        if file_info:
            msg = ("%s: Failure during delete verification: File '%s' still"
                   " exists in container '%s'" %
                   (self.target_type, file_path, self.container_name))
            raise errors.TargetDeleteError(msg)

    ###########################################################################
    def file_exists(self, file_path, expected_file_size=None):
        file_info = self._fetch_file_info(file_path)
        if expected_file_size:
            return file_info and file_info['size'] == expected_file_size
        else:
            return file_info is not None

    ###########################################################################
    def stream_file(self, file_reference):
        url = self.get_temp_download_url(file_reference)
        return requests.get(url, stream=True).iter_lines()

    ###########################################################################
    def _fetch_file_info(self, destination_path):
        """
            Returns a dictionary of file info or None if file does not exist

            The returned dictionary must contain the size of the specified file
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
        doc = super(BackupTarget, self).to_document(display_only=display_only)

        if self.cloud_storage_encryption_enabled:
            doc["cloudStorageEncryptionEnabled"] = self.cloud_storage_encryption_enabled

        if self.preserve is not None:
            doc["preserve"] = self.preserve

        if self.credentials is not None:
            doc["credentials"] = self.credentials.to_document(display_only=display_only)

        if self.tags:
            doc["tags"] = self.tags

        return doc

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

        self._encrypted_access_key = None
        self._encrypted_secret_key = None
        self._is_encrypted_credentials = None

        self._connection = None
        self._bucket = None
        self._region = None

    ###########################################################################
    def do_put_file(self, file_path, destination_path, metadata=None):
        # determine single/multi part upload

        try:
            file_size = os.path.getsize(file_path)

            if file_size >= MULTIPART_MIN_SIZE:
                self._multi_part_put(file_path, destination_path, file_size,
                                     metadata=metadata)
            else:
                self._single_part_put(file_path, destination_path,
                                      metadata=metadata)

            cloud_storage_encryption = self._fetch_file_info(destination_path)['cloud_storage_encryption']

            return FileReference(file_path=destination_path,
                                 file_size=file_size,
                                 cloud_storage_encryption=cloud_storage_encryption)

        except S3ResponseError, sre:
            if 403 == sre.status:
                raise errors.TargetInaccessibleError(self.bucket_name,cause=sre)
            else:
                raise

    ###########################################################################
    def _fetch_file_info(self, destination_path):
        """
            Override by s3 specifics

        """
        bucket = self._get_bucket()

        for key in bucket.list(prefix=destination_path):
            if key.key == destination_path:
                # The 'list' method on the bucket is incorrect and returns Key
                # objects with unset 'encrypted' variables. This is remedied by
                # explicitly calling get_key below. The documentation is also
                # wrong - it implies that 'encrypted' is a boolean when it is
                # actually a string specifying the type of encryption or None
                # if the file is not encrypted.
                # https://github.com/boto/boto/issues/3361
                not_buggy_key = bucket.get_key(key.name)

                return {
                    'size': not_buggy_key.size,
                    'cloud_storage_encryption': not_buggy_key.encrypted,
                    'md5': not_buggy_key.md5,
                    'last_modified': not_buggy_key.last_modified,
                    'metadata': not_buggy_key.metadata,
                    "expiryDate": not_buggy_key.expiry_date,
                    "name": key.name,
                    "storageClass": key.storage_class,
                    "ongoingRestore": key.ongoing_restore
                }

        return None

    ###########################################################################
    def _single_part_put(self, file_path, destination_path, metadata=None):
        bucket = self._get_bucket()
        file_obj = open(file_path)
        k = Key(bucket)
        k.key = destination_path
        # set meta data (has to be before setting content in
        # order for it to work)
        if metadata:
            for name, value in metadata.items():
                k.set_metadata(name, value)

        k.set_contents_from_file(file_obj, encrypt_key=self.cloud_storage_encryption_enabled)


    ###########################################################################
    def _multi_part_put(self, file_path, destination_path, file_size,
                        metadata=None):

        logger.info("S3BucketTarget: Starting multi-part put for %s " %
                    file_path)
        chunk_size = int(file_size / 10)
        if chunk_size > MAX_SPLIT_SIZE:
            chunk_size = MAX_SPLIT_SIZE

        bucket = self._get_bucket()
        mp = bucket.initiate_multipart_upload(destination_path, metadata=metadata,
                                              encrypt_key=self.cloud_storage_encryption_enabled)

        upload = SplitFile(file_path, chunk_size)

        for i, chunk in enumerate(upload, 1):
            logger.info("Uploading file part %d (%s bytes)" %
                         (i, chunk.size))
            mp.upload_part_from_file(chunk, i)

        mp.complete_upload()
        logger.info("S3BucketTarget: Multi-part put for %s completed"
                    " successfully!" % file_path)

    ###########################################################################
    def get_file(self, file_reference, destination):

        file_path = file_reference.file_path

        try:
            file_name = file_reference.file_name

            print("Downloading '%s' from s3 bucket '%s'" %
                  (file_path, self.bucket_name))

            bucket = self._get_bucket()
            key = bucket.get_key(file_path)

            if not key:
                raise errors.TargetFileNotFoundError("No such file '%s' in bucket "
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
            raise errors.TargetError(msg, cause=e)

    ###########################################################################
    def do_delete_file(self, file_reference):
        file_path = file_reference.file_path

        try:
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
        except S3ResponseError, re:
            if 403 == re.status:
                raise errors.TargetInaccessibleError(self.bucket_name,
                                              cause=re)
        except Exception, e:
            if isinstance(e, errors.TargetError):
                raise

            msg = ("S3BucketTarget: Error while trying to delete '%s'"
                   " from s3 bucket %s. Cause: %s" %
                   (file_path, self.bucket_name, e))
            raise errors.TargetDeleteError(msg, cause=e)

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
    @property
    def region(self):
        if not self._region:
            self._connect_to_bucket()

        return self._region

    ###########################################################################
    def _get_bucket(self):
        if not self._bucket:
            self._connect_to_bucket()

        return self._bucket

    ###########################################################################
    def _connect_to_bucket(self):
        if not(self._connection and self._bucket):
            try:
                conn, bucket, region = s3_utils.get_connection_for_bucket(self.get_access_key(),
                                                                          self.get_secret_key(),
                                                                          self.bucket_name)
                self._connection = conn
                self._bucket = bucket
                self._region = region
            except S3ResponseError, re:
                if "403" in safe_stringify(re):
                    raise errors.TargetInaccessibleError(self.bucket_name, cause=re)
                elif "404" in safe_stringify(re):
                    raise errors.NoSuchContainerError(self.bucket_name, cause=re)
                else:
                    raise


    ###########################################################################
    def _get_file_ref_key(self, file_reference):
        file_path = file_reference.file_path

        bucket = self._get_bucket()
        return bucket.get_key(file_path)

    ###########################################################################
    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, access_key):
        self._access_key = str(access_key)

    def get_access_key(self):
        if self.credentials:
            return self.credentials.get_credential("accessKey")
        elif not self.is_use_credential_encryption():
            return self._access_key
        elif self.encrypted_access_key:
            return mbs.get_mbs().encryptor.decrypt_string(
                self.encrypted_access_key)

    ###########################################################################
    @property
    def secret_key(self):
        return self._secret_key

    @secret_key.setter
    def secret_key(self, secret_key):
        self._secret_key = str(secret_key)

        if self.is_use_credential_encryption() and secret_key:
            sak = mbs.get_mbs().encryptor.encrypt_string(str(secret_key))
            self.encrypted_secret_key = sak

    def get_secret_key(self):
        if self.credentials:
            return self.credentials.get_credential("secretKey")
        elif not self.is_use_credential_encryption():
            return self._secret_key
        elif self.encrypted_secret_key:
            return mbs.get_mbs().encryptor.decrypt_string(
                self.encrypted_secret_key)

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
    @property
    def is_encrypted_credentials(self):
        return self._is_encrypted_credentials

    @is_encrypted_credentials.setter
    def is_encrypted_credentials(self, val):
        self._is_encrypted_credentials = val

    def is_use_credential_encryption(self):
        return self.is_encrypted_credentials or self.is_encrypted_credentials is None

    ###########################################################################
    def get_temp_download_url(self, file_reference, expires_in_secs=30):
        bucket = self._get_bucket()
        key = bucket.get_key(file_reference.file_path)
        return key.generate_url(expires_in_secs)

    ###########################################################################
    def is_file_in_glacier(self, file_ref):
        key = self._get_file_ref_key(file_ref)
        return key and key.storage_class == "GLACIER"

    ###########################################################################
    def is_glacier_restore_ongoing(self, file_ref):
        key = self._get_file_ref_key(file_ref)
        return key and key.ongoing_restore

    ###########################################################################
    def is_file_restored(self, file_ref):
        key = self._get_file_ref_key(file_ref)
        return key and key.storage_class == "STANDARD"

    ###########################################################################
    def restore_file_from_glacier(self, file_ref, days=5):
        if self.is_glacier_restore_ongoing(file_ref):
            raise errors.TargetError("Restore already ongoing for file '%s'" %
                              file_ref.file_path)
        elif not self.is_file_in_glacier(file_ref):
            raise errors.TargetError("Restore already ongoing for file '%s'" %
                              file_ref.file_path)

        key = self._get_file_ref_key(file_ref)
        key.restore(days=days)


    ###########################################################################
    def to_document(self, display_only=False):

        doc = BackupTarget.to_document(self, display_only=display_only)

        doc.update({
            "_type": "S3BucketTarget",
            "bucketName": self.bucket_name
        })

        if self.is_encrypted_credentials is not None:
            doc["isEncryptedCredentials"] = self.is_encrypted_credentials

        if not self.is_use_credential_encryption():
            doc.update({
                "accessKey": "xxxxx" if display_only else self._access_key,
                "secretKey": "xxxxx" if display_only else self._secret_key
            })
        else:
            doc.update({
                "encryptedAccessKey": "xxxxx" if display_only else self.encrypted_access_key,
                "encryptedSecretKey": "xxxxx" if display_only else self.encrypted_secret_key
            })

        return doc

    ###########################################################################
    def has_sufficient_permissions(self):
        errors = []
        try:

            bucket = self._get_bucket()

            # test read/write

            # determining current user permissions for a bucket via
            # policies/acls looks to be a mess... try a simple
            # create/write/read/list/delete for now

            key = None
            key_name = '%s-mbs-test-write' % (uuid.uuid4())
            try:
                key = bucket.new_key(key_name)
                key.set_contents_from_string(key_name,
                                             encrypt_key=self.cloud_storage_encryption_enabled)
                if not key.get_contents_as_string() == key_name:
                    errors.append('could not read key contents of test file '
                                  'in %s' % (self.bucket_name))
                # set the prefix to the file name and don't risk listing the
                # whole bucket
                contents = bucket.list(key_name)
                # there should only be one element
                if key_name not in [k.name for k in contents]:
                    errors.append('could not list contents of %s' %
                                  (self.bucket_name))
            except Exception, e:
                logger.exception("has_sufficient_permissions() error")
                errors.append(safe_stringify(e))
            finally:
                if key is not None:
                    try:
                        key.delete()
                    except Exception as e:
                        logger.exception("has_sufficient_permissions() error")
                        errors.append(safe_stringify(e))
        except Exception as e:
            logger.exception("has_sufficient_permissions() error")
            errors.append(safe_stringify(e))

        return errors

    ###########################################################################
    def _validate_bucket_name(self):
        errors = []
        if not self.bucket_name:
            errors.append("Bucket name is required")
        elif not re.match("[a-z0-9][a-z0-9-\.]{1,61}[a-z0-9]$", self.bucket_name):
            errors.append("Bucket name must be between 3 and 63 characters "
                          "long and only contain lowercase letters, numbers, "
                          "hyphens and full stops.")
        elif re.search("\.\.", self.bucket_name):
            errors.append("Bucket name must not contain consecutive full stops")
        elif re.match("\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", self.bucket_name):
            errors.append("Bucket name must not be formatted as an IP address")
        return errors

    ###########################################################################
    def validate(self):
        errors = []
        errors += self._validate_bucket_name()

        if not self.get_access_key():
            errors.append("Access key is required")

        if not self.get_secret_key():
            errors.append("Secret key is required")

        return errors

###############################################################################
# RackspaceCloudFilesTarget
###############################################################################
class RackspaceCloudFilesTarget(BackupTarget):

    ###########################################################################
    def __init__(self):
        BackupTarget.__init__(self)
        self._container_name = None
        self._container = None
        self._encrypted_username = None
        self._encrypted_api_key = None

    ###########################################################################
    def do_put_file(self, file_path, destination_path, metadata=None):

        # determine single/multi part upload
        file_size = os.path.getsize(file_path)

        destination_path = destination_path or os.path.basename(file_path)


        if file_size >= CF_MULTIPART_MIN_SIZE:
            self._multi_part_put(file_path, destination_path, file_size,
                                 metadata=metadata)
        else:
            self._single_part_put(file_path, destination_path,
                                  metadata=metadata)

        return FileReference(file_path=destination_path,
                             file_size=file_size)

    ###########################################################################
    def _single_part_put(self, file_path, destination_path, metadata=None):
        try:

            container = self._get_container()
            container_obj = container.create_object(destination_path)
            container_obj.load_from_filename(file_path)
        except Exception, ex:
            if "unauthorized" in safe_stringify(ex).lower():
                raise errors.TargetConnectionError(self.container_name, ex)
            else:
                raise

    ###########################################################################
    def _multi_part_put(self, file_path, destination_path, file_size,
                        metadata=None):
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
            self.container_name, file_path

        ]
        logger.info("RackspaceCloudFilesTarget: Executing command: %s" %
                    " ".join(st_command))
        execute_command(st_command)
        logger.info("RackspaceCloudFilesTarget: Multi-part put for %s "
                    "completed successfully!" % file_path)


    ###########################################################################
    def _fetch_file_info(self, destination_path):
        container = self._get_container()
        try:
            container_obj = container.get_object(destination_path)
            if container_obj:
                return {'size': container_obj.size}

        except cloudfiles.errors.NoSuchObject:
            pass

        return None

    ###########################################################################
    def get_file(self, file_reference, destination):
        file_path = file_reference.file_path

        try:
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
            raise errors.TargetError(msg, e)

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
            err = safe_stringify(e)
            if "404" in err:
                return False
            if "403" in err:
                raise errors.TargetInaccessibleError(self.container_name, cause=e)

            msg = ("RackspaceCloudFilesTarget: Error while trying to delete "
                   "'%s' from container %s. Cause: %s" %
                   (file_path, self.container_name, e))
            raise errors.TargetDeleteError(msg, e)


    ###########################################################################
    def get_temp_download_url(self, file_reference):
        return cloudfiles_utils.get_download_url(self._get_container(),
                                                 file_reference.file_path)

    ###########################################################################
    @property
    def container_name(self):
        return self._container_name

    @container_name.setter
    def container_name(self, container_name):
        self._container_name = str(container_name)

    ###########################################################################
    def _get_container(self):
        if not self._container:
            try:
                conn = cloudfiles.get_connection(username=self.username,
                                                 api_key=self.api_key,
                                                 timeout=30)

                self._container = conn.get_container(self.container_name)
            except (AuthenticationFailed, NoSuchContainer), e:
                raise errors.TargetInaccessibleError(self.container_name,
                                              cause=e)
        return self._container

    ###########################################################################
    @property
    def username(self):
        if self.credentials:
            return self.credentials.get_credential("username")

        if self.encrypted_username:
            return mbs.get_mbs().encryptor.decrypt_string(self.encrypted_username)

    @username.setter
    def username(self, username):
        if self.credentials:
            self.credentials.set_credential("username", username)
        elif username:
            eu = mbs.get_mbs().encryptor.encrypt_string(str(username))
            self.encrypted_username = eu

    ###########################################################################
    @property
    def api_key(self):
        if self.credentials:
            return self.credentials.get_credential("apiKey")
        if self.encrypted_api_key:
            return mbs.get_mbs().encryptor.decrypt_string(self.encrypted_api_key)

    @api_key.setter
    def api_key(self, api_key):
        if self.credentials:
            self.credentials.set_credential("apiKey", api_key)
        elif api_key:
            eak = mbs.get_mbs().encryptor.encrypt_string(str(api_key))
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
    def has_sufficient_permissions(self):
        return \
            super(RackspaceCloudFilesTarget, self).has_sufficient_permissions()

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
    def put_file(self, file_path, destination_path=None,
                 overwrite_existing=False, metadata=None):
        try:

            # calculating file size
            file_size = os.path.getsize(file_path)
            destination_path = os.path.basename(file_path)

            logger.info("AzureContainerTarget: Uploading %s (%s bytes) "
                        "to container %s" %
                        (file_path, file_size, self.container_name))


            self._single_part_put(file_path, destination_path,
                                  metadata=metadata)

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
    def _single_part_put(self, file_path, destination_path, metadata=None):
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
        return BaseBlobService(account_name=self.account_name,
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
    def to_document(self, display_only=False, export_credentials=False):
        doc = BackupTarget.to_document(self, display_only=display_only,
                                       export_credentials=export_credentials)
        doc.update({
            "_type": "AzureContainerTarget",
            "containerName": self.container_name,
            "accountName": "xxxxx" if display_only else self.account_name,
            "accountKey": "xxxxx" if display_only else self.account_key
        })

        return doc

    ###########################################################################
    def has_sufficient_permissions(self):
        return \
            super(AzureContainerTarget, self).has_sufficient_permissions()

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
        super(TargetReference, self).__init__()

        self._preserve = preserve
        self._deleted_date = None
        self._source_was_locked = None

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
    def source_was_locked(self):
        """
            Set to true if the backup source was locked during backup
        """
        return self._source_was_locked

    @source_was_locked.setter
    def source_was_locked(self, val):
        self._source_was_locked = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {

        }

        if self.preserve is not None:
            doc["preserve"] = self.preserve

        if self.deleted_date:
            doc["deletedDate"] = self.deleted_date

        if self.source_was_locked is not None:
            doc["sourceWasLocked"] = self.source_was_locked

        return doc

    ###########################################################################
    def info(self):
        raise Exception("Need to be overridden")

###############################################################################
# FileReference
###############################################################################
class FileReference(TargetReference):

    ###########################################################################
    def __init__(self, file_path=None, file_size=None, preserve=None, cloud_storage_encryption=None):
        TargetReference.__init__(self, preserve=preserve)
        self._file_path = file_path
        self._file_size = file_size
        self._cloud_storage_encryption = cloud_storage_encryption

    ###########################################################################
    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, file_path):
        self._file_path = file_path

    ###########################################################################
    @property
    def cloud_storage_encryption(self):
        return self._cloud_storage_encryption

    @cloud_storage_encryption.setter
    def cloud_storage_encryption(self, cloud_storage_encryption):
        self._cloud_storage_encryption = cloud_storage_encryption

    ###########################################################################
    @property
    def file_size(self):
        return self._file_size

    @file_size.setter
    def file_size(self, file_size):
        self._file_size = file_size

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
            "fileSize": self.file_size,
            "cloudStorageEncryption": self.cloud_storage_encryption
        })

        return doc

    ###########################################################################
    def info(self):
        return "(File Path: '%s', File Size: '%s')" % (self.file_path,
                                                       self.file_size)

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
        doc = super(CloudBlockStorageSnapshotReference, self).to_document(display_only=display_only)
        doc.update({
            "cloudBlockStorage": self.cloud_block_storage.to_document(display_only=display_only),
            "status": self.status
        })

        return doc


###############################################################################
# CompositeBlockStorageSnapshotReference
###############################################################################

class CompositeBlockStorageSnapshotReference(
    CloudBlockStorageSnapshotReference
):
    """
        Base class for cloud block storage snapshot references
    """
    ###########################################################################
    def __init__(self, cloud_block_storage=None, status=None,
                 constituent_snapshots=None):
        super(CompositeBlockStorageSnapshotReference, self).\
            __init__(cloud_block_storage=cloud_block_storage,
                     status=status)
        self._constituent_snapshots = constituent_snapshots

    ###########################################################################
    @property
    def constituent_snapshots(self):
        return self._constituent_snapshots


    @constituent_snapshots.setter
    def constituent_snapshots(self, val):
        self._constituent_snapshots = val

    ###########################################################################
    @property
    def status(self):
        """
            Override status to return status for all constituent_snapshots
        :return:
        """
        everyone = self.constituent_snapshots
        completed = self._filter_constituents(SnapshotStatus.COMPLETED)
        errored = self._filter_constituents(SnapshotStatus.ERROR)
        pending = self._filter_constituents(SnapshotStatus.PENDING)

        if everyone and completed and len(everyone) == len(completed):
            return SnapshotStatus.COMPLETED
        elif errored:
            return SnapshotStatus.ERROR
        elif pending:
            return SnapshotStatus.PENDING



    @status.setter
    def status(self, status):
        self._status = status

    ###########################################################################
    def _export_constituent_snapshots(self, display_only=False):
        return export_mbs_object_list(self.constituent_snapshots,
                                      display_only=display_only)

    ###########################################################################
    def all_constituents_instance_of(self, check_type):
        return (len(filter(lambda s: isinstance(s, check_type),
                           self.constituent_snapshots)) ==
                len(self.constituent_snapshots))

    ###########################################################################
    def _filter_constituents(self, status):
        snaps = self.constituent_snapshots
        if snaps:
            return filter(lambda s: s.status == status, snaps)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(CompositeBlockStorageSnapshotReference, self).to_document(
            display_only=display_only)

        doc.update({
            "_type": "CompositeBlockStorageSnapshotReference",
            "constituentSnapshots": self._export_constituent_snapshots(
                display_only=display_only)
        })

        return doc

    ###########################################################################
    def info(self):
        const_snap_infos = map(lambda s: s.info(), self.constituent_snapshots)
        return "(Composite Snapshot: [%s])" % ",".join(const_snap_infos)

    ###########################################################################
    def clone(self):
        #TODO XXX this a workaround for some composite snapshot refs where deepcopy
        # done by MBSObject.clone() is erroring with "cannot deepcopy this pattern object"
        # Also, this maybe the proper solution for supe
        import mbs
        return mbs.get_mbs().maker.make(self.to_document())

###############################################################################
# EbsSnapshotReference
###############################################################################
class EbsSnapshotReference(CloudBlockStorageSnapshotReference):

    ###########################################################################
    def __init__(self, snapshot_id=None, cloud_block_storage=None, status=None,
                 volume_size=None, progress=None, start_time=None, encrypted=None):
        CloudBlockStorageSnapshotReference.__init__(self, cloud_block_storage=cloud_block_storage, status=status)
        self._snapshot_id = snapshot_id
        self._volume_size = volume_size
        self._progress = progress
        self._start_time = start_time
        self._share_users = None
        self._share_groups = None
        self._encrypted = encrypted

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
    @property
    def share_users(self):
        return self._share_users

    @share_users.setter
    def share_users(self, val):
        self._share_users = val

    ###########################################################################
    @property
    def share_groups(self):
        return self._share_groups

    @share_groups.setter
    def share_groups(self, val):
        self._share_groups = val


    ###########################################################################
    @property
    def encrypted(self):
        return self._encrypted

    @encrypted.setter
    def encrypted(self, encrypted):
        self._encrypted = encrypted

    ###########################################################################
    def get_ebs_snapshot(self):
        cbs = self.cloud_block_storage
        return cbs.get_ebs_snapshot_by_id(self.snapshot_id)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = CloudBlockStorageSnapshotReference.to_document(self, display_only=display_only)
        doc.update({
            "_type": "EbsSnapshotReference",
            "snapshotId": self.snapshot_id,
            "volumeSize": self.volume_size,
            "progress": self.progress,
            "startTime": self.start_time,
            "encrypted": self.encrypted
        })

        if self.share_users:
            doc["shareUsers"] = self.share_users
        if self.share_groups:
            doc["shareGroups"] = self.share_groups

        return doc

    ###########################################################################
    def info(self):
        return "(Ebs Snapshot: '%s')" % self.snapshot_id


###############################################################################
# BlobSnapshotReference
###############################################################################
class BlobSnapshotReference(CloudBlockStorageSnapshotReference):

    ###########################################################################
    def __init__(self, snapshot_id=None, cloud_block_storage=None, status=None,
                 volume_size=None, progress=None, start_time=None):
        CloudBlockStorageSnapshotReference.__init__(
            self, cloud_block_storage=cloud_block_storage, status=status)
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
        doc = CloudBlockStorageSnapshotReference.to_document(
            self, display_only=display_only)

        doc.update({
            "_type": "BlobSnapshotReference",
            "snapshotId": self.snapshot_id,
            "volumeSize": self.volume_size,
            "progress": self.progress,
            "startTime": self.start_time
        })

        return doc

    ###########################################################################
    def info(self):
        return "(Azure Blob Snapshot: '%s')" % self.snapshot_id


###############################################################################
# ManagedDiskSnapshotReference
###############################################################################
class ManagedDiskSnapshotReference(CloudBlockStorageSnapshotReference):

    ###########################################################################
    def __init__(self, snapshot_id=None, cloud_block_storage=None, status=None,
                 volume_size=None, progress=None, start_time=None):
        CloudBlockStorageSnapshotReference.__init__(
            self, cloud_block_storage=cloud_block_storage, status=status)
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
        doc = CloudBlockStorageSnapshotReference.to_document(
            self, display_only=display_only)

        doc.update({
            "_type": "ManagedDiskSnapshotReference",
            "snapshotId": self.snapshot_id,
            "volumeSize": self.volume_size,
            "progress": self.progress,
            "startTime": self.start_time
        })

        return doc

    ###########################################################################
    def info(self):
        return "(Azure Managed Disk Snapshot: '%s')" % self.snapshot_id


###############################################################################
# GcpDiskSnapshotReference
###############################################################################
class GcpDiskSnapshotReference(CloudBlockStorageSnapshotReference):

    ###########################################################################
    def __init__(self, snapshot_id=None, cloud_block_storage=None, status=None,
                 volume_size=None, progress=None, start_time=None, op=None):
        CloudBlockStorageSnapshotReference.__init__(
            self, cloud_block_storage=cloud_block_storage, status=status)
        self._snapshot_id = snapshot_id
        self._volume_size = volume_size
        self._progress = progress
        self._start_time = start_time
        self._snapshot_op = op

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
        self._volume_size = float(volume_size)

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
    @property
    def snapshot_op(self):
        return self._snapshot_op

    @snapshot_op.setter
    def snapshot_op(self, snapshot_op):
        self._snapshot_op = snapshot_op

    ###########################################################################
    def to_document(self, display_only=False):
        doc = CloudBlockStorageSnapshotReference.to_document(
            self, display_only=display_only)

        doc.update({
            "_type": "GcpDiskSnapshotReference",
            "snapshotId": self.snapshot_id,
            "volumeSize": self.volume_size,
            "progress": self.progress,
            "startTime": self.start_time,
            "snapshot_op": self.snapshot_op
        })

        return doc

    ###########################################################################
    def info(self):
        return "(GCP Disk Snapshot: '%s')" % self.snapshot_id


###############################################################################
# LVMSnapshotReference
###############################################################################
class LVMSnapshotReference(CompositeBlockStorageSnapshotReference):
    ###########################################################################
    def __init__(self, cloud_block_storage=None, constituent_snapshots=None,
                 status=None):
        super(LVMSnapshotReference, self).__init__(cloud_block_storage,
                                                   status,
                                                   constituent_snapshots)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(LVMSnapshotReference, self).to_document(
            display_only=display_only)

        doc.update({
            "_type": "LVMSnapshotReference",
        })

        return doc

    ###########################################################################
    def info(self):
        const_snap_infos = map(lambda s: s.info(), self.constituent_snapshots)
        return "(LVM Snapshot: [%s])" % ",".join(const_snap_infos)

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
                             file_path, **upload_kargs):

    logger.info("MULTI TARGET UPLOAD: Starting concurrent target upload for "
                "file '%s'" % file_path)
    uploaders = []

    # first kick off the uploads
    for target in targets:
        target_uploader = TargetUploader(target, file_path, **upload_kargs)
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

