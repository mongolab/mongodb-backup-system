__author__ = 'abdul'

from base import MBSObject
from errors import BlockStorageSnapshotError
from utils import wait_for
from target import EbsSnapshotReference
from mbs import get_mbs
from errors import *

import mongo_uri_tools
import mbs_logging

from boto.ec2.connection import EC2Connection


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
        self._tags = None
        self._cloud_block_storage = None

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
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        self._tags = tags

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {}

        if self.tags:
            doc["tags"] = self.tags

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
    def __init__(self):
        BackupSource.__init__(self)
        self._uri = None

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
    def create_snapshot(self, description):
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
        self._ec2_connection = None

    ###########################################################################
    def create_snapshot(self, description):
        ebs_volume = self._get_ebs_volume()

        logger.info("Creating EBS snapshot for volume '%s'" % self.volume_id)

        if not ebs_volume.create_snapshot(description):
            raise BlockStorageSnapshotError("Failed to create snapshot from "
                                            "backup source :\n%s" % self)

        # get the snapshot id and put it as a target reference
        ebs_snapshot = self._get_ebs_snapshot_by_desc(description)
        logger.info("Snapshot kicked off successfully for volume '%s'. "
                    "Snapshot id '%s'." % (self.volume_id, ebs_snapshot.id))

        def log_func():
            logger.info("Waiting for snapshot '%s' status to be completed" %
                        ebs_snapshot.id)

        def is_completed():
            ebs_snapshot = self._get_ebs_snapshot_by_desc(description)
            return ebs_snapshot.status == 'completed'

        # log a waiting msg
        log_func() # :)
        # wait until complete
        wait_for(is_completed, timeout=300, log_func=log_func )

        if is_completed():
            logger.info("EBS Snapshot '%s' for volume '%s' completed "
                        "successfully!." % (ebs_snapshot.id, self.volume_id))
            return EbsSnapshotReference(snapshot_id=ebs_snapshot.id,
                                            cloud_block_storage=self)

        else:
            raise BlockStorageSnapshotError("EBS Snapshot Timeout error")

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
    @property
    def volume_id(self):
        return self._volume_id

    @volume_id.setter
    def volume_id(self, volume_id):
        self._volume_id = str(volume_id)

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
            conn = EC2Connection(self.access_key, self.secret_key)
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
            "encryptedAccessKey": ak,
            "encryptedSecretKey": sk
        }