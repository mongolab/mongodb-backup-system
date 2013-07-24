__author__ = 'abdul'

from base import MBSObject
from errors import BlockStorageSnapshotError

from target import EbsSnapshotReference
from mbs import get_mbs
from errors import *

import mongo_uri_tools
import mbs_logging

from boto.ec2 import connect_to_region

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
        self._cloud_block_storage = None
        self._info = None

    ###########################################################################
    @property
    def uri(self):
        pass

    ###########################################################################
    @property
    def database_name(self):
        pass

    ###########################################################################
    @property
    def collection_name(self):
        pass

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
    @property
    def info(self):
        return self._info

    @info.setter
    def info(self, info):
        self._info = info

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {}

        if self.info:
            doc["info"] = self.info

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
        pass

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
        self._region = None
        self._ec2_connection = None

    ###########################################################################
    def create_snapshot(self, name, description):
        ebs_volume = self._get_ebs_volume()

        logger.info("Creating EBS snapshot for volume '%s'" % self.volume_id)

        ebs_snapshot = ebs_volume.create_snapshot(description)
        if not ebs_snapshot:
            raise BlockStorageSnapshotError("Failed to create snapshot from "
                                            "backup source :\n%s" % self)

        # add name tag
        ebs_snapshot.add_tag("Name", name)

        logger.info("Snapshot kicked off successfully for volume '%s'. "
                    "Snapshot id '%s'." % (self.volume_id, ebs_snapshot.id))


        logger.info("EBS Snapshot '%s' for volume '%s' created "
                    "successfully!." % (ebs_snapshot.id, self.volume_id))


        ebs_ref = self._new_ebs_snapshot_reference(ebs_snapshot)

        return ebs_ref

    ###########################################################################
    def delete_snapshot(self, snapshot_ref):
        snapshot_id = snapshot_ref.snapshot_id
        try:
            logger.info("Deleting snapshot '%s' " % snapshot_id)
            self.ec2_connection.delete_snapshot(snapshot_id)
            logger.info("Snapshot '%s' deleted successfully!" % snapshot_id)
        except Exception, e:
            msg = "Error while deleting snapshot '%s'" % snapshot_id
            raise BlockStorageSnapshotError(msg, cause=e)

    ###########################################################################
    def check_snapshot_updates(self, ebs_ref):
        """
            Detects changes in snapshot
        """
        ebs_snapshot = self._get_ebs_snapshot_by_id(ebs_ref.snapshot_id)
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
    def _get_ebs_snapshots(self):
        return self._get_ebs_volume().snapshots()

    ###########################################################################
    def _get_ebs_snapshot_by_desc(self, description):
        snapshots = filter(lambda snapshot: snapshot.description == description,
                      self._get_ebs_snapshots())

        if snapshots:
            return snapshots[0]

    ###########################################################################
    def _get_ebs_snapshot_by_id(self, id):
        snapshots = filter(lambda snapshot: snapshot.id == id,
            self._get_ebs_snapshots())

        if snapshots:
            return snapshots[0]

    ###########################################################################
    def to_document(self, display_only=False):

        ak = "xxxxx" if display_only else self.encrypted_access_key
        sk = "xxxxx" if display_only else self.encrypted_secret_key
        return {
            "_type": "EbsVolumeStorage",
            "volumeId": self.volume_id,
            "region": self.region,
            "encryptedAccessKey": ak,
            "encryptedSecretKey": sk
        }