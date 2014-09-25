__author__ = 'abdul'

from datetime import datetime

from base import MBSObject
from robustify.robustify import retry_till_done, die_with_err, robustify

from target import (
    EbsSnapshotReference, LVMSnapshotReference, BlobSnapshotReference,
    CompositeBlockStorageSnapshotReference, GcpDiskSnapshotReference
    )
from mbs import get_mbs
from errors import *

import mongo_uri_tools
import mbs_logging
import httplib2
import rfc3339

from boto.ec2 import connect_to_region
from azure.storage import BlobService
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.http import HttpRequest, HttpError

from utils import (
    freeze_mount_point, unfreeze_mount_point, export_mbs_object_list,
    suspend_lvm_mount_point, resume_lvm_mount_point, safe_format, random_string
)

import urllib
import time

from mongo_utils import build_mongo_connector

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger


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
    def get_block_storage_by_address(self, address):
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
            raise ConfigurationError(msg)

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
            raise ConfigurationError(msg)

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
    def resource_id(self):
        """
            Must be overridden
        """
        return None

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
    def resource_id(self):
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
# EbsVolumeStorage
###############################################################################
class EbsVolumeStorage(CloudBlockStorage):

    ###########################################################################
    def __init__(self):
        CloudBlockStorage.__init__(self)
        self._encrypted_access_key = None
        self._encrypted_secret_key = None
        self._volume_id = None
        self._volume_name = None
        self._region = None
        self._ec2_connection = None

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
    def do_create_snapshot(self, name, description):
        ebs_volume = self._get_ebs_volume()

        logger.info("Creating EBS snapshot (name='%s', desc='%s') for volume "
                    "'%s' (%s)" %
                    (name, description, self.volume_id, self.volume_name))

        ebs_snapshot = ebs_volume.create_snapshot(description)
        if not ebs_snapshot:
            raise BlockStorageSnapshotError("Failed to create snapshot from "
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

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=5,
               do_on_exception=raise_if_not_ec2_retriable,
               do_on_failure=raise_exception)
    def _set_ebs_snapshot_name(self, ebs_snapshot, name):
        ebs_snapshot.add_tag("Name", name)

    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        snapshot_id = snapshot_ref.snapshot_id
        try:
            logger.info("Deleting snapshot '%s' " % snapshot_id)
            self.ec2_connection.delete_snapshot(snapshot_id)
            logger.info("Snapshot '%s' deleted successfully!" % snapshot_id)
            return True
        except Exception, e:
            if ("does not exist" in str(e) or
                "InvalidSnapshot.NotFound" in str(e)):
                logger.warning("Snapshot '%s' does not exist" % snapshot_id)
                return False
            else:
                msg = "Error while deleting snapshot '%s'" % snapshot_id
                raise BlockStorageSnapshotError(msg, cause=e)

    ###########################################################################
    def check_snapshot_updates(self, ebs_ref):
        """
            Detects changes in snapshot
        """
        ebs_snapshot = self.get_ebs_snapshot_by_id(ebs_ref.snapshot_id)
        # NOTE check if the above call returns a snapshot object because boto
        # returns None although the snapshot exists (AWS api freakiness ?)
        if ebs_snapshot:
            new_ebs_ref = self._new_ebs_snapshot_reference(ebs_snapshot)
            if new_ebs_ref != ebs_ref:
                return new_ebs_ref

    ###########################################################################
    def _new_ebs_snapshot_reference(self, ebs_snapshot):
        return EbsSnapshotReference(snapshot_id=ebs_snapshot.id,
                                    cloud_block_storage=self,
                                    status=ebs_snapshot.status,
                                    start_time=ebs_snapshot.start_time,
                                    volume_size=ebs_snapshot.volume_size,
                                    progress=ebs_snapshot.progress)

    ###########################################################################
    @property
    def volume_id(self):
        return self._volume_id

    @volume_id.setter
    def volume_id(self, volume_id):
        self._volume_id = str(volume_id)

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
        elif self.encrypted_access_key:
            return get_mbs().encryptor.decrypt_string(
                self.encrypted_access_key)

    @access_key.setter
    def access_key(self, access_key):
        if self.credentials:
            self.credentials.set_credential("accessKey", access_key)
        elif access_key:
            eak = get_mbs().encryptor.encrypt_string(str(access_key))
            self.encrypted_access_key = eak

    ###########################################################################
    @property
    def secret_key(self):
        if self.credentials:
            return self.credentials.get_credential("secretKey")
        elif self.encrypted_secret_key:
            return get_mbs().encryptor.decrypt_string(
                self.encrypted_secret_key)

    @secret_key.setter
    def secret_key(self, secret_key):
        if self.credentials:
            self.credentials.set_credential("secretKey", secret_key)
        elif secret_key:
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
    @property
    def ec2_connection(self):
        if not self._ec2_connection:
            conn = connect_to_region(self.region,
                                     aws_access_key_id=self.access_key,
                                     aws_secret_access_key=self.secret_key)
            if not conn:
                raise ConfigurationError("Invalid region in block storage %s" %
                                         self)
            self._ec2_connection = conn

        return self._ec2_connection


    ###########################################################################
    def _get_ebs_volume(self):
        volumes = self.ec2_connection.get_all_volumes([self.volume_id])

        if volumes is None or len(volumes) == 0:
            raise Exception("Could not find volume %s" % self.volume_id)

        return volumes[0]

    ###########################################################################
    def get_ebs_snapshots(self):
        filters = {
            "volume-id": self.volume_id
        }
        return self.ec2_connection.get_all_snapshots(filters=filters)

    ###########################################################################
    def get_ebs_snapshot_by_id(self, snapshot_id):
        filters = {
            "volume-id": self.volume_id,
            "snapshot-id": snapshot_id
        }
        snapshots= self.ec2_connection.get_all_snapshots(filters=filters)

        if snapshots:
            return snapshots[0]

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

        ak = "xxxxx" if display_only else self.encrypted_access_key
        sk = "xxxxx" if display_only else self.encrypted_secret_key
        doc.update({
            "_type": "EbsVolumeStorage",
            "volumeId": self.volume_id,
            "volumeName": self.volume_name,
            "region": self.region,
            "encryptedAccessKey": ak,
            "encryptedSecretKey": sk
        })

        return doc


###############################################################################
# BlobStorage
###############################################################################
class BlobVolumeStorage(CloudBlockStorage):

    ###########################################################################
    def __init__(self):
        CloudBlockStorage.__init__(self)
        self._encrypted_access_key = None
        self._storage_account = None
        self._volume_id = None
        self._volume_name = None
        self._blob_service_connection = None

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
            container_name, blob_name, x_ms_meta_name_values=metadata)

        if not response:
            raise BlockStorageSnapshotError("Failed to create snapshot from "
                                            "backup source :\n%s" % self)

        logger.info("Snapshot successfully created for volume '%s' (%s). "
                    "Snapshot id '%s'." % (self.volume_id, self.volume_name,
                                           response['x-ms-snapshot']))

        # let's grab the snapshot
        blob_ref = None

        blobs = self.blob_service_connection.list_blobs(
            container_name, prefix=blob_name, include="snapshots")
        for blob in blobs:
            if blob.snapshot == response['x-ms-snapshot']:
                blob_ref = self._new_blob_snapshot_reference(blob)
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
            self.blob_service_connection.delete_blob(
                container_name, blob_name, snapshot=snapshot_time)

            return True
        except Exception, e:
            msg = "Error while deleting snapshot '%s'" % snapshot_id
            logger.exception(msg)
            raise BlockStorageSnapshotError(msg, cause=e)

    ###########################################################################
    def _new_blob_snapshot_reference(self, blob_snapshot):

        start_time_str = blob_snapshot.properties.last_modified
        start_time = datetime.strptime(start_time_str,
                                       "%a, %d %b %Y %H:%M:%S %Z")

        return BlobSnapshotReference(
            snapshot_id=blob_snapshot.url,
            cloud_block_storage=self,
            status="completed",
            start_time=start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
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
        elif self.encrypted_access_key:
            return get_mbs().encryptor.decrypt_string(
                self.encrypted_access_key)

    @access_key.setter
    def access_key(self, access_key):
        if self.credentials:
            self.credentials.set_credential("accessKey", access_key)
        elif access_key:
            eak = get_mbs().encryptor.encrypt_string(str(access_key))
            self.encrypted_access_key = eak

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
    def blob_service_connection(self):
        if not self._blob_service_connection:
            self.validate()
            logger.info("Creating connection to blob service for "
                        "volume '%s'" % self.volume_id)
            conn = BlobService(account_name=self.storage_account,
                               account_key=self.access_key)

            logger.info("SUCCESS!!! Connection created successfully to blob "
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
            raise ConfigurationError("BlobVolumeStorage: storage account is "
                                     "not set")
        if not self.access_key:
            raise ConfigurationError("BlobVolumeStorage: access key is not set")
    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(BlobVolumeStorage, self).to_document(
            display_only=display_only)

        ak = "xxxxx" if display_only else self.encrypted_access_key

        doc.update({
            "_type": "BlobVolumeStorage",
            "volumeId": self.volume_id,
            "volumeName": self.volume_name,
            "storageAccount": self.storage_account,
            "encryptedAccessKey": ak
        })

        return doc


###############################################################################
# GcpDiskVolumeStorage
###############################################################################
class GcpDiskVolumeStorage(CloudBlockStorage):

    ###########################################################################
    def __init__(self):
        CloudBlockStorage.__init__(self)
        self._encrypted_service_account_name = None
        self._encrypted_private_key = None
        # self._project = None
        self._zone = None
        self._volume_id = None
        self._volume_name = None
        self._gce_service_connection = None

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

        snapshot_op = self.gce_service_connection.disks().createSnapshot(
            project=self.credentials.get_credential('projectId'),
            zone=self.zone,
            disk=self.volume_id,
            body={
                "description": description,
                "name": m_name
            }
        ).execute(num_retries=3)

        if not snapshot_op or \
                ('warnings' in snapshot_op and
                         len(snapshot_op['warnings']) > 0) or \
                ('error' in snapshot_op and
                         len(snapshot_op['error']['errors']) > 0):
            raise BlockStorageSnapshotError("Failed to create snapshot from "
                                            "backup source :\n%s\n%s" %
                                            (self, snapshot_op))

        logger.info("Snapshot successfully created for volume '%s' (%s). "
                    "Snapshot id '%s'." % (self.volume_id, self.volume_name,
                                           snapshot_op['selfLink']))

        snapshot = self.get_disk_snapshot_by_name(m_name)

        if snapshot:
            return self._new_disk_snapshot_reference(snapshot, snapshot_op)
        else:
            raise BlockStorageSnapshotError("Could not locate the newly "
                                            "created snapshot w/ name: %s"
                                            % m_name)

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
                logger.info("Snapshot '%s' deleted successfully!" % snapshot_id)
                return True
            else:
                msg = "Snapshot '%s' was not deleted! Error: %s" \
                      % (snapshot_id, op_result['error'])
                raise RetriableError(msg)
        except Exception, e:
            msg = "Error while deleting snapshot '%s'" % snapshot_id
            logger.exception(msg)
            raise BlockStorageSnapshotError(msg, cause=e)

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
            raise BlockStorageSnapshotError("Failed to create snapshot from "
                                            "backup source :\n%s\n%s" %
                                            (self, snapshot_op))

        if disk_snapshot and snapshot_op:
            new_snapshot_ref = self._new_disk_snapshot_reference(disk_snapshot,
                                                             snapshot_op)
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

        return GcpDiskSnapshotReference(snapshot_id=disk_snapshot['name'],
                                     cloud_block_storage=self,
                                     status=status,
                                     start_time=start_time.strftime(
                                         "%Y-%m-%dT%H:%M:%S.000Z"),
                                     volume_size=float(
                                         disk_snapshot['diskSizeGb']),
                                     progress=snapshot_op['progress'],
                                     op=snapshot_op)

    ###########################################################################
    @property
    def zone(self):
        return self._zone

    @zone.setter
    def zone(self, zone):
        self._zone = str(zone)

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
    def private_key(self):
        if self.credentials:
            return self.credentials.get_credential("privateKey")
        elif self.encrypted_private_key:
            return get_mbs().encryptor.decrypt_string(
                self.encrypted_private_key)

    @private_key.setter
    def private_key(self, private_key):
        if self.credentials:
            self.credentials.set_credential("privateKey", private_key)
        elif private_key:
            epk = get_mbs().encryptor.encrypt_string(str(private_key))
            self.encrypted_private_key = epk

    ###########################################################################
    @property
    def encrypted_private_key(self):
        return self._encrypted_private_key

    @encrypted_private_key.setter
    def encrypted_private_key(self, val):
        if val:
            self._encrypted_private_key = val.encode('ascii', 'ignore')

    ###########################################################################
    @property
    def service_account_name(self):
        if self.credentials:
            return self.credentials.get_credential("serviceAccountName")
        elif self.encrypted_service_account_name:
            return get_mbs().encryptor.decrypt_string(
                self.encrypted_service_account_name)

    @service_account_name.setter
    def service_account_name(self, service_account_name):
        if self.credentials:
            self.credentials.set_credential("serviceAccountName",
                                            service_account_name)
        elif service_account_name:
            esan = get_mbs().encryptor.encrypt_string(str(service_account_name))
            self.encrypted_service_account_name = esan

    ###########################################################################
    @property
    def encrypted_service_account_name(self):
        return self._encrypted_service_account_name

    @encrypted_service_account_name.setter
    def encrypted_service_account_name(self, val):
        if val:
            self._encrypted_service_account_name = val.encode('ascii', 'ignore')

    ###########################################################################
    @property
    def gce_service_connection(self):
        if not self._gce_service_connection:
            logger.info("Creating connection to GCE service for "
                        "volume '%s'" % self.volume_id)

            key = self.credentials.get_credential("privateKey")
            service_account_name = \
                self.credentials.get_credential('serviceAccountName')
            credentials = SignedJwtAssertionCredentials(
                service_account_name,
                key,
                scope='https://www.googleapis.com/auth/compute')
            http = httplib2.Http()
            http = credentials.authorize(http)

            self._gce_service_connection = build(
                'compute', 'v1', http=http, requestBuilder=RobustHttpRequest)

        return self._gce_service_connection

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

        pk = "xxxxx" if display_only else self.encrypted_private_key
        serviceAccountName = "xxxxx" if display_only else \
            self.encrypted_service_account_name
        doc.update({
            "_type": "GcpDiskVolumeStorage",
            "volumeId": self.volume_id,
            "volumeName": self.volume_name,
            # "projectId": self.project,
            "zone": self.zone,
            "serviceAccountName": serviceAccountName,
            "encryptedPrivateKey": pk
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
            return CompositeBlockStorageSnapshotReference(
                self, constituent_snapshots=new_constituent_snapshots)


    ###########################################################################
    def do_create_snapshot(self, name_template, description_template):
        """
            Creates a LVMSnapshotReference composed of all
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
    def check_snapshot_updates(self, snapshot_ref):
        composite_ref = super(LVMStorage, self).check_snapshot_updates(
            snapshot_ref)

        if composite_ref:
            return LVMSnapshotReference(
                self,
                constituent_snapshots=composite_ref.constituent_snapshots)

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
        })

        return doc


class RobustHttpRequest(HttpRequest):

    def execute(self, http=None, num_retries=0, do_on_exception=None):

        # bring on the robustness
        if do_on_exception is None:
            do_on_exception = lambda e: logger.warning(e)

        return retry_till_done(
            lambda: super(RobustHttpRequest, self).execute(http=http, num_retries=num_retries),
            max_attempts=3,
            do_on_exception=do_on_exception)
