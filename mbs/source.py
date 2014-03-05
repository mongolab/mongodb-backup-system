__author__ = 'abdul'

from datetime import datetime

from base import MBSObject
from errors import BlockStorageSnapshotError

from target import EbsSnapshotReference, LVMSnapshotReference, BlobSnapshotReference
from mbs import get_mbs
from errors import *

import mongo_uri_tools
import mbs_logging

from boto.ec2 import connect_to_region
from azure.storage import BlobService
from utils import (
    freeze_mount_point, unfreeze_mount_point, export_mbs_object_list,
    suspend_lvm_mount_point, resume_lvm_mount_point, safe_format
)
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
    def to_document(self, display_only=False):
        doc =  super(MongoSource, self).to_document()
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
        """
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
    def create_snapshot(self, name, description):
        ebs_volume = self._get_ebs_volume()

        logger.info("Creating EBS snapshot (name='%s', desc='%s') for volume "
                    "'%s' (%s)" %
                    (name, description, self.volume_id, self.volume_name))

        ebs_snapshot = ebs_volume.create_snapshot(description)
        if not ebs_snapshot:
            raise BlockStorageSnapshotError("Failed to create snapshot from "
                                            "backup source :\n%s" % self)

        # add name tag
        ebs_snapshot.add_tag("Name", name)

        logger.info("Snapshot kicked off successfully for volume '%s' (%s). "
                    "Snapshot id '%s'." % (self.volume_id, self.volume_name,
                                           ebs_snapshot.id))

        ebs_ref = self._new_ebs_snapshot_reference(ebs_snapshot)

        return ebs_ref

    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        snapshot_id = snapshot_ref.snapshot_id
        try:
            logger.info("Deleting snapshot '%s' " % snapshot_id)
            self.ec2_connection.delete_snapshot(snapshot_id)
            logger.info("Snapshot '%s' deleted successfully!" % snapshot_id)
            return True
        except Exception, e:
            if "does not exist" in str(e):
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
    def create_snapshot(self, name, description):

        logger.info("Creating blob snapshot (name='%s', desc='%s') for volume "
                    "'%s' (%s)" %
                    (name, description, self.volume_id, self.volume_name))

        container_name, blob_name = \
            self._get_container_and_blob_names_from_media_link(self.volume_id)

        metadata = {"name": name, "description": description}

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
    def _new_blob_snapshot_reference(self, blob_snapshot):

        start_time_str = blob_snapshot.properties.last_modified
        start_time = datetime.strptime(start_time_str,
                                       "%a, %d %b %Y %H:%M:%S %Z")

        return BlobSnapshotReference(snapshot_id=blob_snapshot.url,
                                     cloud_block_storage=self,
                                     status="completed",
                                     start_time=start_time.strftime(
                                         "%Y-%m-%dT%H:%M:%S.000Z"),
                                     volume_size=blob_snapshot.properties.
                                     content_length / (1024 * 1024 * 1024),
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
        if self.encrypted_access_key:
            return get_mbs().encryptor.decrypt_string(self.encrypted_access_key)

    @access_key.setter
    def access_key(self, access_key):
        if access_key:
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
            conn = BlobService(account_name=self.storage_account,
                               account_key=self.access_key)

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
    def to_document(self, display_only=False):
        doc = super(CompositeBlockStorage, self).to_document(
            display_only=display_only)

        doc.update({
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
    def create_snapshot(self, name_template, description_template):
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
            return LVMSnapshotReference(self,
                                        constituent_snapshots=
                                        new_constituent_snapshots)

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
