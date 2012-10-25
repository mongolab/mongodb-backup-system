__author__ = 'abdul'

from base import MBSObject
from utils import document_pretty_string, is_cluster_mongo_uri, is_mongo_uri

from boto.ec2.connection import EC2Connection


###############################################################################
# Backup Source Classes
###############################################################################
class BackupSource(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    @property
    def source_uri(self):
        pass

    ###########################################################################
    def get_current_stats(self):
        pass

    ###########################################################################
    def is_cluster_source(self):
        return False

    ###########################################################################
    def to_document(self):
        pass

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
# SourceStats
###############################################################################
class SourceStats(MBSObject):

    ###########################################################################
    def __init__(self):
        self._last_optime = None
        self._repl_lag = None

    ###########################################################################
    @property
    def last_optime(self):
        return self._last_optime

    @last_optime.setter
    def last_optime(self, last_optime):
        self._last_optime = last_optime

    ###########################################################################
    @property
    def repl_lag(self):
        return self._repl_lag

    @repl_lag.setter
    def repl_lag(self, repl_lag):
        self._repl_lag = repl_lag

    ###########################################################################
    def to_document(self):
        doc = {
            "_type": "SourceStats"
        }
        if self.last_optime:
            doc["lastOptime"] = self.last_optime

        if self.repl_lag:
            doc["replLag"] = self.repl_lag

        return doc

###############################################################################
# Database Source
###############################################################################
class MongoSource(BackupSource):

    ###########################################################################
    def __init__(self):
        BackupSource.__init__(self)
        self._database_address = None

    ###########################################################################
    @property
    def database_address(self):
        return self._database_address

    @database_address.setter
    def database_address(self, address):
        self._database_address = address

    ###########################################################################
    @property
    def source_uri(self):
        return self.database_address

    ###########################################################################
    def to_document(self):
        return {
            "_type": "MongoSource",
            "databaseAddress": self.database_address
        }

    ###########################################################################
    def is_cluster_source(self):
        return is_cluster_mongo_uri(self.source_uri)


    ###########################################################################
    def validate(self):
        errors = []
        if not self.database_address:
            errors.append("Missing 'databaseAddress' property")
        elif not is_mongo_uri(self.database_address):
            errors.append("Invalid 'databaseAddress'.%s" % e)

        return errors

###############################################################################
# EbsVolumeSource
###############################################################################
class EbsVolumeSource(BackupSource):

    ###########################################################################
    def __init__(self):
        BackupSource.__init__(self)
        self._access_key = None
        self._secret_key = None
        self._ec2_connection = None
        self._volume_id = None

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
    def get_volume(self):
        volumes = self.ec2_connection.get_all_volumes([self.volume_id])

        if volumes is None or len(volumes) == 0:
            raise Exception("Could not find volume %s" % self.volume_id)

        return volumes[0]

    ###########################################################################
    def get_snapshots(self):
        return self.get_volume().snapshots()

    ###########################################################################
    def get_snapshot_by_desc(self, description):
        snapshots = filter(lambda snapshot: snapshot.description == description,
                      self.get_snapshots())

        if snapshots:
            return snapshots[0]

    ###########################################################################
    def to_document(self):
        return {
            "_type": "EbsVolumeSource",
            "volumeId": self.volume_id,
            "accessKey": self.access_key,
            "secretKey": self.secret_key
        }