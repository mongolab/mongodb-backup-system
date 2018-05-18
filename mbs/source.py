__author__ = 'abdul'

from datetime import datetime, timedelta

from base import MBSObject
from robustify.robustify import (
    retry_till_done, die_with_err, robustify, wait_for
)

from target import (
    EbsSnapshotReference, LVMSnapshotReference, BlobSnapshotReference,
    CompositeBlockStorageSnapshotReference, GcpDiskSnapshotReference,
    ManagedDiskSnapshotReference
    )
from mbs import get_mbs
import errors as mbs_errors

import mongo_uri_tools
import logging
import httplib2
import rfc3339
import date_utils

from boto.ec2 import connect_to_region
from azure.storage.blob.baseblobservice import BaseBlobService
from azure.mgmt.compute import ComputeManagementClient
from azure.common import AzureMissingResourceHttpError
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute.models import DiskCreateOption
from msrestazure.azure_exceptions import CloudError
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.http import HttpRequest, HttpError

from utils import (
    freeze_mount_point, unfreeze_mount_point, export_mbs_object_list, safe_stringify,
    suspend_lvm_mount_point, resume_lvm_mount_point, safe_format, random_string
)

import urllib
import time

from mongo_utils import build_mongo_connector

from repoze.lru import lru_cache

import boto.ec2.snapshot
###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

###############################################################################
# Backup Source Classes
###############################################################################
class BackupSource(MBSObject):

    ###########################################################################
    def __init__(self):
        MBSObject.__init__(self)
        self._cloud_block_storage = None

    ###########################################################################
    @property
    def uri(self):
        return None

    ###########################################################################
    @property
    def database_name(self):
        return None

    ###########################################################################
    @property
    def collection_name(self):
        return None

    ###########################################################################
    @property
    def cloud_block_storage(self):
        """
            OPTIONAL: Represents cloud block storage for the source
        """
        return self._cloud_block_storage

    @cloud_block_storage.setter
    def cloud_block_storage(self, val):
        self._cloud_block_storage = val

    ###########################################################################
    def get_block_storage_by_connector(self, connector):
        address = connector.address
        block_storage = self.cloud_block_storage
        if block_storage is None:
            return None
        elif isinstance(block_storage, dict):
            return block_storage.get(address)
        elif isinstance(block_storage, CloudBlockStorage):
            return block_storage
        else:
            msg = ("Invalid cloudBlockStorageConfig. Must be a "
                   "CloudBlockStorage or a dict of address=>CloudBlockStorage")
            raise mbs_errors.ConfigurationError(msg)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {}

        if self.cloud_block_storage:
            doc["cloudBlockStorage"] = self._export_cloud_block_storage()

        return doc

    ###########################################################################
    def _export_cloud_block_storage(self, display_only=False):
        cbs = self.cloud_block_storage
        if isinstance(cbs, CloudBlockStorage):
            return cbs.to_document(display_only=display_only)
        elif isinstance(cbs, dict):
            return dict((key, value.to_document(display_only=display_only))
                            for (key, value) in cbs.items())
        else:
            msg = ("Invalid cloudBlockStorageConfig. Must be a "
                   "CloudBlockStorage or a dict of address=>CloudBlockStorage")
            raise mbs_errors.ConfigurationError(msg)

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
    def get_source_info(self):
        """
        :return: an info string about the source
        """

    ###########################################################################
    @property
    def id(self):
        """
            Must be overridden
        """
        return None

    ###########################################################################
    def get_connector(self):
        """
            must be implemented
        """
        raise Exception("get_connector() must be implemented")

    ###########################################################################
    def get_selected_sources(self, selected_connector):
        """
            must be implemented
        """
        raise Exception("get_selected_sources() must be implemented")

###############################################################################
# MongoSource
###############################################################################
class MongoSource(BackupSource):

    ###########################################################################
    def __init__(self, uri=None):
        BackupSource.__init__(self)
        self._uri = uri

    ###########################################################################
    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        self._uri = uri

    ###########################################################################
    def get_connector(self):
        return build_mongo_connector(self.uri)

    ###########################################################################
    def get_selected_sources(self, selected_connector):
        """
            returns a single array of self!!!
        """
        return [self]

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(MongoSource, self).to_document()
        doc.update ({
            "_type": "MongoSource",
            "uri": (mongo_uri_tools.mask_mongo_uri(self.uri) if display_only
                    else self.uri)
        })

        return doc

    ###########################################################################
    def validate(self):
        errors = []
        if not self.uri:
            errors.append("Missing 'uri' property")
        elif not mongo_uri_tools.is_mongo_uri(self.uri):
            errors.append("Invalid uri '%s'" % self.uri)

        return errors

    ###########################################################################
    @property
    def id(self):
        return mongo_uri_tools.mask_mongo_uri(self.uri)

###############################################################################
# CloudBlockStorageSource
###############################################################################
class CloudBlockStorage(MBSObject):
    """
        Base class for Cloud Block Storage
    """
    ###########################################################################
    def __init__(self):
        MBSObject.__init__(self)
        self._mount_point = None
        self._credentials = None

    ###########################################################################
    @property
    def credentials(self):
        return self._credentials

    @credentials.setter
    def credentials(self, val):
        self._credentials = val

    ###########################################################################
    def create_snapshot(self, name, description):

        name = safe_format(name, cbs=self)
        description = safe_format(description, cbs=self)
        # encode description to ascii
        description = description and description.encode('ascii', 'ignore')

        return self.do_create_snapshot(name, description)

    ###########################################################################
    def do_create_snapshot(self, name, description):
        """
            Does the actual work
            Create a snapshot for the volume with the specified description.
            Returns a CloudBlockStorageSnapshotReference
             Must be implemented by subclasses
        """

    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        """
            deletes the snapshot reference
            Must be implemented by subclasses
        """

    ###########################################################################
    def check_snapshot_updates(self, snapshot_ref):
        """
            Checks status updates to snapshot and populates reference with new
            updates.
            Returns true if there were new updates
        """

    ###########################################################################
    def suspend_io(self):
        """
           suspends the underlying IO
        """

    ###########################################################################
    def resume_io(self):
        """
            resumes the underlying IO
        """

    ###########################################################################
    @property
    def mount_point(self):
        return self._mount_point

    @mount_point.setter
    def mount_point(self, val):
        self._mount_point = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "mountPoint": self.mount_point
        }

        if self.credentials:
            doc["credentials"] = self.credentials.to_document(
                display_only=display_only)

        return doc


###############################################################################
# VolumeStorage
###############################################################################
class VolumeStorage(CloudBlockStorage):
    ###########################################################################
    def __init__(self):
        CloudBlockStorage.__init__(self)
        self._cloud_id = None
        self._volume_id = None
        self._volume_name = None
        self._volume_size = None
        self._fs_type = None
        self._volume_encrypted = None

    ###########################################################################
    @property
    def cloud_id(self):
        return self._cloud_id

    @cloud_id.setter
    def cloud_id(self, val):
        self._cloud_id = val

    ###########################################################################
    @property
    def volume_id(self):
        return self._volume_id

    @volume_id.setter
    def volume_id(self, volume_id):
        self._volume_id = str(volume_id)

    ###########################################################################
    @property
    def volume_name(self):
        return self._volume_name

    @volume_name.setter
    def volume_name(self, val):
        self._volume_name = str(val)

    ###########################################################################
    @property
    def volume_size(self):
        return self._volume_size

    @volume_size.setter
    def volume_size(self, volume_size):
        self._volume_size = volume_size

    ###########################################################################
    @property
    def fs_type(self):
        return self._fs_type

    @fs_type.setter
    def fs_type(self, val):
        self._fs_type = val

    ###########################################################################
    @property
    def volume_encrypted(self):
        return self._volume_encrypted

    @volume_encrypted.setter
    def volume_encrypted(self, val):
        self._volume_encrypted = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(VolumeStorage, self).to_document(display_only=display_only)

        doc.update({
            "volumeId": self.volume_id,
            "volumeName": self.volume_name,
            "volumeSize": self.volume_size,
            "cloudId": self.cloud_id,
            "fsType": self.fs_type,
            "volumeEncrypted": self.volume_encrypted
        })

        return doc


###############################################################################
# EbsVolumeStorage
###############################################################################
class EbsVolumeStorage(VolumeStorage):

    ###########################################################################
    def __init__(self):
        VolumeStorage.__init__(self)
        self._region = None

    ###########################################################################
    def do_create_snapshot(self, name, description):
        try:


            logger.info("EC2: BEGIN Creating EBS snapshot (name='%s', desc='%s') for volume "
                        "'%s' (%s)" % (name, description, self.volume_id, self.volume_name))

            start_date = date_utils.date_now()

            ebs_snapshot = custom_ebs_create_snapshot(self.ec2_connection, self.volume_id, description)

            # log elapsed time for aws call
            elapsed_time = date_utils.timedelta_total_seconds(date_utils.date_now() - start_date)
            logger.info("EC2: END create snapshot for snapshot '%s' volume '%s' returned in %s seconds" %
                        (ebs_snapshot.id, self.volume_id , elapsed_time))

            if not ebs_snapshot:
                raise mbs_errors.BlockStorageSnapshotError("Failed to create snapshot from "
                                                           "backup source :\n%s" % self)

            logger.info("Snapshot kicked off successfully for volume '%s' (%s). "
                        "Snapshot id '%s'." % (self.volume_id, self.volume_name,
                                               ebs_snapshot.id))
            # add name tag
            logger.info("Setting snapshot '%s' name to '%s'" % (
                ebs_snapshot.id, name))

            # sleep for a couple of seconds before setting name
            time.sleep(2)

            self._set_ebs_snapshot_name(ebs_snapshot, name)

            ebs_ref = self._new_ebs_snapshot_reference(ebs_snapshot)

            return ebs_ref
        except Exception, ex:
            if "ConcurrentSnapshotLimitExceeded" in safe_stringify(ex):
                raise mbs_errors.Ec2ConcurrentSnapshotLimitExceededError("Maximum allowed in-progress snapshots "
                                                                         "for volume exceeded.", cause=ex)
            else:
                raise

    ###########################################################################
    @robustify(max_attempts=5, retry_interval=5,
               do_on_exception=mbs_errors.raise_if_not_ec2_retriable,
               do_on_failure=mbs_errors.raise_exception)
    def _set_ebs_snapshot_name(self, ebs_snapshot, name):

        logger.info("EC2: BEGIN setting snapshot name for snapshot '%s' volume '%s'" %
                    (ebs_snapshot.id, self.volume_id))
        start_date = date_utils.date_now()

        ebs_snapshot.add_tag("Name", name)
        # log elapsed time for aws call
        elapsed_time = date_utils.timedelta_total_seconds(date_utils.date_now() - start_date)
        logger.info("EC2: END set snapshot name for snapshot '%s' volume '%s' returned in %s seconds" %
                    (ebs_snapshot.id, self.volume_id , elapsed_time))


    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        snapshot_id = snapshot_ref.snapshot_id
        try:
            logger.info("EC2: BEGIN Deleting snapshot '%s' " % snapshot_id)

            snapshot_deleted = False
            for i in xrange(10):
                self.ec2_connection.delete_snapshot(snapshot_id)
                snapshot_deleted = not self.snapshot_exists(snapshot_id)
                if snapshot_deleted:
                    break
                else:
                    # sleep for one second
                    time.sleep(1)

            if not snapshot_deleted:
                raise mbs_errors.Ec2SnapshotDeleteError("Snapshot '%s' still exists after 10 attempts of delete!" %
                                                        snapshot_id)
            logger.info("EC2: END Snapshot '%s' deleted successfully!" % snapshot_id)
            return True
        except Exception, e:
            if ("does not exist" in safe_stringify(e) or
                "InvalidSnapshot.NotFound" in safe_stringify(e)):
                logger.warning("Snapshot '%s' does not exist" % snapshot_id)
                return False
            else:
                msg = "Error while deleting snapshot '%s'" % snapshot_id
                logger.exception(msg)
                raise mbs_errors.BlockStorageSnapshotError(msg, cause=e)

    ###########################################################################
    @robustify(max_attempts=1, retry_interval=5,
               do_on_exception=mbs_errors.raise_if_not_ec2_retriable,
               do_on_failure=mbs_errors.raise_exception,
               backoff=2)
    def check_snapshot_updates(self, ebs_ref):
        """
            Detects changes in snapshot
        """
        try:
            ebs_snapshot = self.get_ebs_snapshot_by_id(ebs_ref.snapshot_id)

            # NOTE check if the above call returns a snapshot object because boto
            # returns None although the snapshot exists (AWS api freakiness ?)
            if ebs_snapshot:
                new_ebs_ref = self.new_ebs_snapshot_reference_from_existing(ebs_ref, ebs_snapshot)
                if new_ebs_ref != ebs_ref:
                    return new_ebs_ref
            else:
                raise mbs_errors.Ec2SnapshotDoesNotExistError("Snapshot %s does not exist!" % ebs_ref.snapshot_id)
        except Exception, e:
            if (not isinstance(e, mbs_errors.Ec2SnapshotDoesNotExistError) and
                        "InvalidSnapshot.NotFound" in safe_stringify(e)):
                raise mbs_errors.Ec2SnapshotDoesNotExistError("Snapshot %s does not exist!" % ebs_ref.snapshot_id)
            else:
                raise


    ###########################################################################
    @robustify(max_attempts=1, retry_interval=5,
               do_on_exception=mbs_errors.raise_if_not_ec2_retriable,
               do_on_failure=mbs_errors.raise_exception,
               backoff=2)
    def share_snapshot(self, ebs_ref, user_ids=None, groups=None):
        if not user_ids and not groups:
            raise ValueError("must specify user_ids or groups")

        ebs_snapshot = self.get_ebs_snapshot_by_id(ebs_ref.snapshot_id)
        if not ebs_snapshot:
            raise mbs_errors.Ec2SnapshotDoesNotExistError("EBS snapshot '%s' does not exist" % ebs_ref.snapshot_id)

        # remove dashes from user ids
        if user_ids:
            user_ids = map(lambda user_id: user_id.replace("-",""), user_ids)

        try:
            logger.info("EC2: BEGIN Sharing snapshot '%s with users %s, groups %s' " %
                        (ebs_ref.snapshot_id, user_ids, groups))
            ebs_snapshot.share(user_ids=user_ids, groups=groups)
            logger.info("EC2: END Snapshot '%s' shared with users %s, groups %s successfully!" %
                        (ebs_ref.snapshot_id, user_ids, groups))
            if user_ids:
                ebs_ref.share_users = ebs_ref.share_users or list()
                if not set(user_ids).issubset(set(ebs_ref.share_users)):
                    ebs_ref.share_users.extend(user_ids)

            if groups:
                ebs_ref.share_groups = ebs_ref.share_groups or list()
                if not set(groups).issubset(set(ebs_ref.share_groups)):
                    ebs_ref.share_groups.extend(groups)

            return ebs_ref
        except mbs_errors.BotoServerError, bte:
            raise mbs_errors.Ec2Error("Failed to share snapshot: %s" % bte, cause=bte)

    ###########################################################################
    def _new_ebs_snapshot_reference(self, ebs_snapshot):
        return EbsSnapshotReference(snapshot_id=ebs_snapshot.id,
                                    cloud_block_storage=self,
                                    status=ebs_snapshot.status,
                                    start_time=ebs_snapshot.start_time,
                                    volume_size=ebs_snapshot.volume_size,
                                    progress=ebs_snapshot.progress,
                                    encrypted=ebs_snapshot.encrypted)

    ###########################################################################
    def new_ebs_snapshot_reference_from_existing(self, ebs_ref, ebs_snapshot):
        new_ebs_ref = ebs_ref.clone()
        new_ebs_ref.status = ebs_snapshot.status
        new_ebs_ref.start_time = ebs_snapshot.start_time
        new_ebs_ref.volume_size = ebs_snapshot.volume_size
        new_ebs_ref.progress = ebs_snapshot.progress
        return new_ebs_ref

    ###########################################################################
    @property
    def region(self):
        return self._region

    @region.setter
    def region(self, region):
        self._region = str(region)

    ###########################################################################
    @property
    def access_key(self):
        if self.credentials:
            return self.credentials.get_credential("accessKey")

    ###########################################################################
    @property
    def secret_key(self):
        if self.credentials:
            return self.credentials.get_credential("secretKey")

    ###########################################################################
    @property
    def ec2_connection(self):
        return cached_ec2_connect_to_region(self.region, self.access_key, self.secret_key)

    ###########################################################################


    @region.setter
    def region(self, region):
        self._region = str(region)

    ###########################################################################
    def get_ebs_snapshots(self):
        filters = {
            "volume-id": self.volume_id
        }
        return self.ec2_connection.get_all_snapshots(filters=filters)

    ###########################################################################
    def get_ebs_snapshot_by_id(self, snapshot_id):
        filters = {
            "snapshot-id": snapshot_id
        }

        start_date = date_utils.date_now()

        logger.info("EC2: BEGIN get snapshot '%s' for  volume '%s'" % (snapshot_id, self.volume_id))
        snapshots = self.ec2_connection.get_all_snapshots(filters=filters)
        # log elapsed time for aws call
        elapsed_time = date_utils.timedelta_total_seconds(date_utils.date_now() - start_date)
        logger.info("EC2: END get snapshot '%s' for  volume '%s' returned in %s seconds" %
                    (snapshot_id, self.volume_id , elapsed_time))
        if snapshots:
            return snapshots[0]

    ###########################################################################
    def snapshot_exists(self, snapshot_id):
        return self.get_ebs_snapshot_by_id(snapshot_id) is not None

    ###########################################################################
    def suspend_io(self):
        logger.info("Suspend IO for volume '%s' using fsfreeze" %
                    self.volume_id)
        freeze_mount_point(self.mount_point)

    ###########################################################################
    def resume_io(self):

        logger.info("Resume io for volume '%s' using fsfreeze" %
                    self.volume_id)

        unfreeze_mount_point(self.mount_point)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(EbsVolumeStorage, self).to_document(display_only=
                                                        display_only)

        doc.update({
            "_type": "EbsVolumeStorage",
            "region": self.region
        })

        return doc


###############################################################################
# BlobStorage
###############################################################################
class BlobVolumeStorage(VolumeStorage):

    ###########################################################################
    def __init__(self):
        VolumeStorage.__init__(self)
        self._encrypted_access_key = None
        self._storage_account = None
        self._blob_service_connection = None
        self.volume_encrypted = False

    ###########################################################################
    def do_create_snapshot(self, name, description):

        logger.info("Creating blob snapshot (name='%s', desc='%s') for volume "
                    "'%s' (%s)" %
                    (name, description, self.volume_id, self.volume_name))

        container_name, blob_name = \
            self._get_container_and_blob_names_from_media_link(self.volume_id)

        metadata = {"name": urllib.quote(name),
                    "description": urllib.quote(description)}

        response = self.blob_service_connection.snapshot_blob(
            container_name, blob_name, metadata=metadata)

        if not response:
            raise mbs_errors.BlockStorageSnapshotError("Failed to create snapshot from backup source :\n%s" % self)

        logger.info("Snapshot successfully created for volume '%s' (%s). "
                    "Snapshot id '%s'." % (self.volume_id, self.volume_name,
                                           response.snapshot))

        # let's grab the snapshot
        blob_ref = None

        blobs = self.blob_service_connection.list_blobs(
            container_name, prefix=blob_name, include="snapshots")
        for blob in blobs:
            if blob.snapshot == response.snapshot:
                url = self.blob_service_connection.make_blob_url(container_name, blob_name) + \
                      ("?snapshot=%s" % urllib.quote(blob.snapshot))
                blob_ref = self._new_blob_snapshot_reference(blob, url)
                break

        return blob_ref

    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        snapshot_id = snapshot_ref.snapshot_id
        try:
            logger.info("Deleting snapshot '%s' " % snapshot_id)

            [media_link, snapshot] = snapshot_id.split('?')
            container_name, blob_name = \
                self._get_container_and_blob_names_from_media_link(media_link)
            snapshot_time = urllib.unquote(snapshot.split('=')[1])
            logger.info("About to delete snapshot '%s' for '%s/%s'" %
                        (snapshot_time, container_name, blob_name))
            self.blob_service_connection.delete_blob(
                container_name, blob_name, snapshot=snapshot_time)

            return True
        except AzureMissingResourceHttpError:
            logger.warning("Snapshot '%s' does not exist" % snapshot_id)
            return False

        except Exception, e:
            msg = "Error while deleting snapshot '%s'" % snapshot_id
            logger.exception(msg)
            raise mbs_errors.BlockStorageSnapshotError(msg, cause=e)

    ###########################################################################
    def _new_blob_snapshot_reference(self, blob_snapshot, url):

        return BlobSnapshotReference(
            snapshot_id=url,
            cloud_block_storage=self,
            status="completed",
            start_time=blob_snapshot.properties.last_modified.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            volume_size=(blob_snapshot.properties.content_length /
                         (1024 * 1024 * 1024)),
            progress="100%")

    ###########################################################################
    @staticmethod
    def _get_container_and_blob_names_from_media_link(media_link):

        [container_name, blob_name] = media_link.rsplit('/', 2)[-2:]
        return container_name, blob_name

    ###########################################################################
    @property
    def storage_account(self):
        return self._storage_account

    @storage_account.setter
    def storage_account(self, storage_account):
        self._storage_account = str(storage_account)

    ###########################################################################
    @property
    def access_key(self):
        if self.credentials:
            return self.credentials.get_credential("accessKey")

    @access_key.setter
    def access_key(self, access_key):
        if self.credentials:
            self.credentials.set_credential("accessKey", access_key)

    ###########################################################################
    @property
    def blob_service_connection(self):
        if not self._blob_service_connection:
            self.validate()
            logger.info("Creating connection to blob service for "
                        "volume '%s'" % self.volume_id)
            conn = BaseBlobService(account_name=self.storage_account,
                                   account_key=self.access_key)

            logger.info("Connection created successfully to blob "
                        "service for volume '%s'" % self.volume_id)
            self._blob_service_connection = conn

        return self._blob_service_connection

    ###########################################################################
    def suspend_io(self):
        # todo: move this up the parent?
        logger.info("Suspend IO for volume '%s' using fsfreeze" %
                    self.volume_id)
        freeze_mount_point(self.mount_point)

    ###########################################################################
    def resume_io(self):
        # todo: move this up the parent?
        logger.info("Resume io for volume '%s' using fsfreeze" %
                    self.volume_id)

        unfreeze_mount_point(self.mount_point)

    ###########################################################################
    def validate(self):
        if not self.storage_account:
            raise mbs_errors.ConfigurationError("BlobVolumeStorage: storage account is not set")
        if not self.access_key:
            raise mbs_errors.ConfigurationError("BlobVolumeStorage: access key is not set")

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(BlobVolumeStorage, self).to_document(display_only=display_only)

        doc.update({
            "_type": "BlobVolumeStorage",
            "storageAccount": self.storage_account
        })

        return doc


###############################################################################
# Managed Disks
###############################################################################
class ManagedDiskVolumeStorage(VolumeStorage):

    ###########################################################################
    def __init__(self):
        VolumeStorage.__init__(self)
        self._encrypted_access_key = None
        self._location = None
        self._compute_client = None
        self.volume_encrypted = True

    ###########################################################################
    def do_create_snapshot(self, name, description):

        logger.info("Creating managed disk snapshot (name='%s', desc='%s') for volume "
                    "'%s' (%s)" %
                    (name, description, self.volume_id, self.volume_name))

        metadata = {"name": urllib.quote(name),
                    "description": urllib.quote(description)}

        async_snapshot_creation = self.compute_client.snapshots.create_or_update(
            resource_group_name=self.volume_id.split('/')[4],
            snapshot_name=name,
            snapshot={
                'location': self.location,
                'creation_data': {
                    'create_option': DiskCreateOption.copy,
                    'source_uri': self.volume_id
                },
                'tags': metadata
            }
        )

        snapshot = async_snapshot_creation.result()

        if not snapshot:
            raise mbs_errors.BlockStorageSnapshotError("Failed to create snapshot from backup source :\n%s" % self)

        logger.info("Snapshot successfully created for volume '%s' (%s). "
                    "Snapshot id '%s'." % (self.volume_id, self.volume_name,
                                           snapshot.id))

        return self._new_managed_disk_snapshot_reference(snapshot)

    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        snapshot_name = snapshot_ref.snapshot_id.split('/')[-1]
        try:
            logger.info("Deleting snapshot '%s' " % snapshot_ref.snapshot_id)

            async_snapshot_creation = self.compute_client.snapshots.delete(
                resource_group_name=self.volume_id.split('/')[4],
                snapshot_name=snapshot_name
            )

            async_snapshot_creation.wait()

            return True
        except CloudError, ce:
            if 'NotFound' in ce.error.error:
                logger.warning("Snapshot '%s' does not exist" % snapshot_ref.snapshot_id)
                return False
            else:
                msg = "Error while deleting snapshot '%s'" % snapshot_ref.snapshot_id
                logger.exception(msg)
                raise mbs_errors.BlockStorageSnapshotError(msg, cause=ce)

    ###########################################################################
    def _new_managed_disk_snapshot_reference(self, snapshot):

        return ManagedDiskSnapshotReference(
            snapshot_id=snapshot.id,
            cloud_block_storage=self,
            status="completed",
            start_time=snapshot.time_created.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            volume_size=snapshot.disk_size_gb,
            progress="100%")

    ###########################################################################
    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, location):
        self._location = str(location)

    ###########################################################################
    @property
    def compute_client(self):
        if not self._compute_client:
            self.validate()
            logger.info("Creating compute client to Azure (ARM) Compute API for "
                        "volume '%s'" % self.volume_id)

            sp_creds = ServicePrincipalCredentials(self.credentials.get_credential('clientId'),
                                                   self.credentials.get_credential('clientSecret'),
                                                   tenant=self.credentials.get_credential('tenantId'))
            compute_client = ComputeManagementClient(sp_creds, str(self.credentials.get_credential('subscriptionId')))

            logger.info("Client created successfully to Azure (ARM) Compute API "
                        "service for volume '%s'" % self.volume_id)
            self._compute_client = compute_client

        return self._compute_client

    ###########################################################################
    def suspend_io(self):
        # todo: move this up the parent?
        logger.info("Suspend IO for volume '%s' using fsfreeze" %
                    self.volume_id)
        freeze_mount_point(self.mount_point)

    ###########################################################################
    def resume_io(self):
        # todo: move this up the parent?
        logger.info("Resume io for volume '%s' using fsfreeze" %
                    self.volume_id)

        unfreeze_mount_point(self.mount_point)

    ###########################################################################
    def validate(self):
        if not self.location:
            raise mbs_errors.ConfigurationError("ManagedDiskVolumeStorage: location is not set")

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(ManagedDiskVolumeStorage, self).to_document(display_only=display_only)

        doc.update({
            "_type": "ManagedDiskVolumeStorage",
            "location": self.location
        })

        return doc


###############################################################################
# GcpDiskVolumeStorage
###############################################################################
class GcpDiskVolumeStorage(VolumeStorage):

    _gce_svc_conn_life_secs = 300

    ###########################################################################
    def __init__(self):
        VolumeStorage.__init__(self)
        self._zone = None
        self._volume_encrypted = True
        self._gce_svc_cached_conn = None
        self._gce_svc_conn_expires_at = None

    ###########################################################################
    def do_create_snapshot(self, name, description):

        # hack to get around google's strict naming conventions
        # (add 'm-' prefix):
        m_name = 'm-%s-%s' % (name, random_string(2).lower())
        while self.snapshot_exists(m_name):
            m_name = 'm-%s-%s' % (name, random_string(2).lower())

        logger.info("Creating disk snapshot (name='%s', desc='%s') for volume "
                    "'%s' (%s)" %
                    (m_name, description, self.volume_id, self.volume_name))

        snapshot_op = self._initiate_snapshot_op(m_name, description)

        if not snapshot_op or \
                ('warnings' in snapshot_op and
                         len(snapshot_op['warnings']) > 0) or \
                ('error' in snapshot_op and
                         len(snapshot_op['error']['errors']) > 0):
            raise mbs_errors.BlockStorageSnapshotError("Failed to create snapshot from backup source :\n%s\n%s" %
                                                       (self, snapshot_op))

        def snapshot_exists():
            return self.snapshot_exists(m_name)

        def on_wait_for_snapshot():
            logger.info("Waiting for snapshot '%s' to exist..." % m_name)

        timeout = 120
        wait_for(snapshot_exists, timeout=timeout, on_wait=on_wait_for_snapshot)

        if not snapshot_exists():
            raise mbs_errors.BlockStorageSnapshotError("Timed out waiting for snapshot '%s' to exist!" % m_name)

        snapshot = self.get_disk_snapshot_by_name(m_name)
        snapshot_op = self.get_snapshot_op(snapshot_op)
        return self._new_disk_snapshot_reference(snapshot, snapshot_op)

    ###########################################################################
    def _initiate_snapshot_op(self, snapshot_name, description):

        snapshot_op = self.gce_service_connection.disks().createSnapshot(
            project=self.credentials.get_credential('projectId'),
            zone=self.zone,
            disk=self.volume_id,
            body={
                "description": description,
                "name": snapshot_name
            }
        ).execute(num_retries=3)

        # wait for the op to be in either "RUNNING" or "DONE" state
        def snapshot_op_in_progress():
            op = self.get_snapshot_op(snapshot_op)
            return 'status' in op and op['status'] in ['RUNNING', 'DONE']

        def on_wait_for_snapshot_op():
            logger.info("Waiting for snapshot op '%s' to enter 'RUNNING' or "
                        "'DONE' state..." % snapshot_op['id'])

        timeout = 600
        wait_for(snapshot_op_in_progress, timeout=timeout,
                 on_wait=on_wait_for_snapshot_op)

        if not snapshot_op_in_progress():
            # still?!
            raise mbs_errors.BlockStorageSnapshotError("Timed out waiting %s seconds for snapshot '%s' to begin!" %
                                                       (timeout, snapshot_name))

        return self.get_snapshot_op(snapshot_op)

    ###########################################################################
    def snapshot_exists(self, name):
        """
        Check if a snapshot with specified name exists.
        :param name: Name of the snapshot to look for
        :return: True or False
        """

        def raise_if_404(err):
            if isinstance(err, HttpError) and err.resp.status == 404:
                die_with_err(err)

        try:
            snapshot = self.gce_service_connection.snapshots().get(
                project=self.credentials.get_credential('projectId'),
                snapshot=name
            ).execute(num_retries=3, do_on_exception=raise_if_404)

            return snapshot is not None
        except HttpError, error:
            if error.resp.status == 404:
                logger.info("snapshot '%s' does not exist" % name)
                return False
            else:
                logger.warning(error)

        return False

    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        snapshot_id = snapshot_ref.snapshot_id
        try:
            logger.info("Deleting snapshot '%s' " % snapshot_id)

            # let's check if it exists first
            if self.snapshot_exists(snapshot_id):

                delete_op = self.gce_service_connection.snapshots().delete(
                    project=self.credentials.get_credential('projectId'),
                    snapshot=snapshot_id
                ).execute(num_retries=3)

                def log_stuff():
                    logger.info("Waiting for async GCP snapshot delete op to "
                                "finish...")

                request = self.gce_service_connection.globalOperations().get(
                    project=self.credentials.get_credential('projectId'),
                    operation=delete_op['name'])

                op_result = retry_till_done(
                    lambda: request.execute(num_retries=3),
                    is_good=lambda result: result['status'] == 'DONE',
                    max_wait_in_secs=300,
                    do_between_attempts=log_stuff,
                    do_on_failure=lambda: die_with_err(
                        'Timed out after waiting %s seconds for operation to '
                        'finish {operation_id : %s}' % (300, delete_op['name'])),
                    retry_interval=5
                )

                if 'error' not in op_result:
                    logger.info("Snapshot '%s' deleted successfully!" %
                                snapshot_id)
                    return True
                else:
                    msg = "Snapshot '%s' was not deleted! Error: %s" \
                          % (snapshot_id, op_result['error'])
                    raise mbs_errors.RetriableError(msg)
            else:
                logger.warning("Not deleting snapshot '%s' because it doesn't "
                               "exist!" % snapshot_id)
                # return True because nothing to delete
                return True

        except Exception, e:
            msg = "Error while deleting snapshot '%s'" % snapshot_id
            logger.exception(msg)
            raise mbs_errors.BlockStorageSnapshotError(msg, cause=e)

    ###########################################################################
    def check_snapshot_updates(self, snapshot_ref):
        """
            Detects changes in snapshot
        """
        disk_snapshot = self.get_disk_snapshot_by_name(snapshot_ref.snapshot_id)
        snapshot_op = self.get_snapshot_op(snapshot_ref.snapshot_op)

        if not snapshot_op or \
                ('warnings' in snapshot_op and
                         len(snapshot_op['warnings']) > 0) or \
                ('error' in snapshot_op and
                         len(snapshot_op['error']['errors']) > 0):
            raise mbs_errors.BlockStorageSnapshotError("Failed to create snapshot from backup source :\n%s\n%s" %
                                                       (self, snapshot_op))

        if disk_snapshot and snapshot_op:
            new_snapshot_ref = self.new_disk_snapshot_reference_from_existing(snapshot_ref, disk_snapshot, snapshot_op)
            if new_snapshot_ref != snapshot_ref:
                return new_snapshot_ref

    ###########################################################################
    def get_disk_snapshot_by_name(self, snapshot_name):

        snapshot = self.gce_service_connection.snapshots().get(
            project=self.credentials.get_credential('projectId'),
            snapshot=snapshot_name
        ).execute(num_retries=3)

        return snapshot

    ###########################################################################
    def get_snapshot_op(self, snapshot_op):

        if 'zone' in snapshot_op:
            zone_name = snapshot_op['zone'].split('/')[-1]
            request = self.gce_service_connection.zoneOperations().get(
                project=self.credentials.get_credential('projectId'),
                operation=snapshot_op['name'],
                zone=zone_name)
        else:
            request = self.gce_service_connection.globalOperations().get(
                project=self.credentials.get_credential('projectId'),
                operation=snapshot_op['name'])

        snapshot_op = request.execute(num_retries=3)
        return snapshot_op

    ###########################################################################
    def _new_disk_snapshot_reference(self, disk_snapshot, snapshot_op):

        start_time_str = disk_snapshot['creationTimestamp']
        start_time = rfc3339.parse_datetime(start_time_str)

        # status needs to be one of ['pending', 'completed', 'error']
        if disk_snapshot['status'] in ['CREATING', 'UPLOADING']:
            status = 'pending'
        elif disk_snapshot['status'] in ['READY']:
            status = 'completed'
        elif disk_snapshot['status'] in ['FAILED']:
            status = 'error'
        else:
            raise Exception('GCP disk snapshot in unhandled state: %s'
                            % disk_snapshot['status'])

        # progress is an optional field on the snapshot_op, don't rely on it
        if 'progress' in snapshot_op:
            progress = snapshot_op['progress']
        else:
            progress = None

        return GcpDiskSnapshotReference(snapshot_id=disk_snapshot['name'],
                                        cloud_block_storage=self,
                                        status=status,
                                        start_time=start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                                        volume_size=float(disk_snapshot['diskSizeGb']),
                                        progress=progress,
                                        op=snapshot_op)

    ###########################################################################
    def new_disk_snapshot_reference_from_existing(self, snapshot_ref, disk_snapshot, snapshot_op):
        new_snapshot_ref = snapshot_ref.clone()
        updated_snapshot_ref = self._new_disk_snapshot_reference(disk_snapshot, snapshot_op)
        new_snapshot_ref.snapshot_op = updated_snapshot_ref.snapshot_op
        new_snapshot_ref.status = updated_snapshot_ref.status
        new_snapshot_ref.start_time = updated_snapshot_ref.start_time
        new_snapshot_ref.volume_size = updated_snapshot_ref.volume_size
        new_snapshot_ref.progress = updated_snapshot_ref.progress

        return new_snapshot_ref

    ###########################################################################
    @property
    def zone(self):
        return self._zone

    @zone.setter
    def zone(self, zone):
        self._zone = str(zone)

    ###########################################################################
    @property
    def private_key(self):
        if self.credentials:
            return self.credentials.get_credential("privateKey")

    @private_key.setter
    def private_key(self, private_key):
        if self.credentials:
            self.credentials.set_credential("privateKey", private_key)


    ###########################################################################
    @property
    def service_account_name(self):
        if self.credentials:
            return self.credentials.get_credential("serviceAccountName")

    @service_account_name.setter
    def service_account_name(self, service_account_name):
        if self.credentials:
            self.credentials.set_credential("serviceAccountName", service_account_name)

    ###########################################################################
    @property
    @robustify(max_attempts=3, backoff=2)
    def gce_service_connection(self):
        if not self._gce_svc_cached_conn or \
                self._gce_connection_is_expired():
            logger.info("Creating connection to GCE service...")

            key = self.credentials.get_credential("privateKey")
            service_account_name = \
                self.credentials.get_credential('serviceAccountName')
            credentials = SignedJwtAssertionCredentials(
                service_account_name,
                key,
                scope='https://www.googleapis.com/auth/compute')
            http = httplib2.Http()
            http = credentials.authorize(http)

            # possible for this to error out... wrapping function in robustify
            self._gce_svc_cached_conn = build(
                'compute', 'v1', http=http, requestBuilder=RobustHttpRequest)

            self._gce_svc_conn_expires_at = datetime.utcnow() + \
                timedelta(seconds=GcpDiskVolumeStorage._gce_svc_conn_life_secs)

        return self._gce_svc_cached_conn

    ###########################################################################
    def _gce_connection_is_expired(self):
        delta = self._gce_svc_conn_expires_at - datetime.utcnow()
        return delta.total_seconds() <= 0

    ###########################################################################
    def suspend_io(self):
        # todo: move this up the parent?
        logger.info("Suspend IO for volume '%s' using fsfreeze" %
                    self.volume_id)
        freeze_mount_point(self.mount_point)

    ###########################################################################
    def resume_io(self):
        # todo: move this up the parent?
        logger.info("Resume io for volume '%s' using fsfreeze" %
                    self.volume_id)

        unfreeze_mount_point(self.mount_point)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(GcpDiskVolumeStorage, self).to_document(
            display_only=display_only)

        doc.update({
            "_type": "GcpDiskVolumeStorage",
            # "projectId": self.project,
            "zone": self.zone
        })

        return doc


###############################################################################
# CompositeBlockStorage
###############################################################################
class CompositeBlockStorage(CloudBlockStorage):
    """
        Base class for Block Storage composed of other storage
    """

    ###########################################################################
    def __init__(self):
        CloudBlockStorage.__init__(self)
        self._constituents = None
    ###########################################################################
    @property
    def constituents(self):
        return self._constituents


    @constituents.setter
    def constituents(self, val):
        self._constituents = val

    ###########################################################################
    def _export_constituents(self, display_only=False):
        return export_mbs_object_list(self.constituents,
                                      display_only=display_only)

    ###########################################################################
    def _create_constituent_snapshots(self, name_template,
                                      description_template):
        constituent_snapshots = []
        for constituent in self.constituents:
            logger.info("Creating snapshot constituent: \n%s" %
                        str(constituent))
            name = safe_format(name_template, constituent=constituent)
            desc = safe_format(description_template, constituent=constituent)

            snapshot = constituent.create_snapshot(name, desc)
            constituent_snapshots.append(snapshot)

        return constituent_snapshots


    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        for (constituent,
             constituent_snapshot) in zip(self.constituents,
                                          snapshot_ref.constituent_snapshots):
            constituent.delete_snapshot(constituent_snapshot)


    ###########################################################################
    def check_snapshot_updates(self, snapshot_ref):
        new_constituent_snapshots = []
        has_changes = False
        for (constituent,
             constituent_snapshot) in zip(self.constituents,
                                          snapshot_ref.constituent_snapshots):
            new_constituent_snapshot = \
                constituent.check_snapshot_updates(constituent_snapshot)
            if new_constituent_snapshot:
                has_changes = True
            else:
                new_constituent_snapshot = constituent_snapshot

            new_constituent_snapshots.append(new_constituent_snapshot)

        if has_changes:
            new_ref = snapshot_ref.clone()
            new_ref.constituent_snapshots = new_constituent_snapshots
            return new_ref


    ###########################################################################
    def do_create_snapshot(self, name_template, description_template):
        """
            Creates a CompositeBlockStorageSnapshotReference composed of all
            constituent snapshots
        """
        logger.info("Creating Composite Snapshot name='%s', description='%s' "
                    "for CompositeBlockStorage: \n%s" %
                    (name_template, description_template, str(self)))

        logger.info("Creating snapshots for all constituents...")

        constituent_snapshots = self._create_constituent_snapshots(
            name_template, description_template)

        composite_snapshot = CompositeBlockStorageSnapshotReference(
            self, constituent_snapshots=constituent_snapshots)

        logger.info("Successfully created Composite Snapshot \n%s" %
                    str(composite_snapshot))

        return composite_snapshot

    ###########################################################################
    def suspend_io(self):
        logger.info("Suspending Composite block storage: Running suspend "
                    "across all constituents...")
        for constituent in self.constituents:
            constituent.suspend_io()

    ###########################################################################
    def resume_io(self):
        logger.info("Resuming Composite block storage: Running resume "
                    "across all constituents...")
        for constituent in self.constituents:
            constituent.resume_io()

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(CompositeBlockStorage, self).to_document(
            display_only=display_only)

        doc.update({
            "_type": "CompositeBlockStorage",
            "constituents": self._export_constituents(
                display_only=display_only)
        })

        return doc

###############################################################################
# LVMStorage
###############################################################################
class LVMStorage(CompositeBlockStorage):
    ###########################################################################
    def __init__(self):
        CompositeBlockStorage.__init__(self)
        self._volume_size = None
        self._fs_type = None


    ###########################################################################
    @property
    def volume_size(self):
        return self._volume_size

    @volume_size.setter
    def volume_size(self, volume_size):
        self._volume_size = volume_size

    ###########################################################################
    @property
    def fs_type(self):
        return self._fs_type

    @fs_type.setter
    def fs_type(self, val):
        self._fs_type = val

    ###########################################################################
    def do_create_snapshot(self, name_template, description_template):
        """
            Creates a LVMSnapshotReference composed of all
            constituent snapshots
        """
        logger.info("Creating LVM Snapshot name='%s', description='%s' "
                    "for LVMStorage: \n%s" % (name_template,
                                              description_template, str(self)))
        logger.info("Creating snapshots for all constituents...")

        constituent_snapshots = \
            self._create_constituent_snapshots(name_template,
                                               description_template)
        lvm_snapshot = LVMSnapshotReference(self,
                                            constituent_snapshots=
                                            constituent_snapshots)

        logger.info("Successfully created LVM Snapshot \n%s" %
                    str(lvm_snapshot))

        return lvm_snapshot

    ###########################################################################
    def suspend_io(self):
        logger.info("Suspend IO for LVM '%s' using dmsetup" %
                    self.mount_point)
        suspend_lvm_mount_point(self.mount_point)

    ###########################################################################
    def resume_io(self):

        logger.info("Resume io for LVM '%s' using dmsetup" %
                    self.mount_point)

        resume_lvm_mount_point(self.mount_point)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(LVMStorage, self).to_document(
            display_only=display_only)

        doc.update({
            "_type": "LVMStorage",
            "volumeSize": self.volume_size,
            "fsType": self.fs_type
        })

        return doc

########################################################################################################################
class RobustHttpRequest(HttpRequest):

    def execute(self, http=None, num_retries=0, do_on_exception=None):

        # bring on the robustness
        if do_on_exception is None:
            do_on_exception = lambda e: logger.warning(e)

        return retry_till_done(
            lambda: super(RobustHttpRequest, self).execute(http=http, num_retries=num_retries),
            max_attempts=3,
            do_on_exception=do_on_exception,
            do_on_failure=mbs_errors.raise_exception)


########################################################################################################################
@lru_cache(100, timeout=60*60)
def cached_ec2_connect_to_region(region, aws_access_key_id, aws_secret_access_key):
    logger.info("EC2: BEGIN Create connection to region '%s'" % region)
    start_date = date_utils.date_now()

    conn = connect_to_region(region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    # log elapsed time for aws call
    elapsed_time = date_utils.timedelta_total_seconds(date_utils.date_now() - start_date)
    logger.info("EC2: END Create connection to region '%s' returned in %s seconds" % (region, elapsed_time))

    if not conn:
        raise mbs_errors.ConfigurationError("Failed to create ec2 connection to region '%s'" % region)

    return conn


########################################################################################################################
def custom_ebs_create_snapshot(ec2_connection, volume_id, description=None):
    """
        Custom ec2 create snapshot
    """
    params = {'VolumeId': volume_id}
    if description:
        params['Description'] = description[0:255]

    snapshot = ec2_connection.get_object('CreateSnapshot', params, boto.ec2.snapshot.Snapshot, verb='POST')

    return snapshot


########################################################################################################################
@robustify(max_attempts=5, retry_interval=5,
           do_on_exception=mbs_errors.raise_if_not_retriable,
           do_on_failure=mbs_errors.raise_exception)
def share_snapshot(snapshot_ref, user_ids=None, groups=None):
    if isinstance(snapshot_ref, CompositeBlockStorageSnapshotReference):
        logger.info("Sharing All constituent snapshots for composite snapshot: %s" % snapshot_ref)
        for cs in snapshot_ref.constituent_snapshots:
            cs.cloud_block_storage.share_snapshot(cs, user_ids=user_ids, groups=groups)
    elif isinstance(snapshot_ref, EbsSnapshotReference):
        snapshot_ref.cloud_block_storage.share_snapshot(snapshot_ref, user_ids=user_ids, groups=groups)
    else:
        raise ValueError("Cannot share a non EBS/Composite snapshot %s" % snapshot_ref)

    logger.info("Snapshot %s shared successfully!" % snapshot_ref)

    return snapshot_ref

########################################################################################################################
def is_snapshot_volume_encrypted(snapshot_ref):
    if isinstance(snapshot_ref, EbsSnapshotReference):
        return snapshot_ref.cloud_block_storage.volume_encrypted
    elif isinstance(snapshot_ref, CompositeBlockStorageSnapshotReference):
        encrypted_constituents = filter(is_snapshot_volume_encrypted, snapshot_ref.constituent_snapshots)
        return len(encrypted_constituents) > 0

    return False