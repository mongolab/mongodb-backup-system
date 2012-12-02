__author__ = 'abdul'

from base import MBSObject
import mongo_uri_tools

from boto.ec2.connection import EC2Connection


###############################################################################
# Backup Source Classes
###############################################################################
class BackupSource(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def get_source_info(self, **kwargs):
        """
            returns a dict that contains source address and stats
        """
        return {
            "address": self.get_source_address(**kwargs),
            "stats": self.get_current_stats(**kwargs)
        }
    ###########################################################################
    def get_source_address(self, **kwargs):
        pass

    ###########################################################################
    def get_current_stats(self, **kwargs):
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
# Database Source
###############################################################################
class MongoSource(BackupSource):

    ###########################################################################
    def __init__(self):
        BackupSource.__init__(self)
        self._uri_wrapper = None

    ###########################################################################
    @property
    def uri(self):
        if self.uri_wrapper:
            return self.uri_wrapper.raw_uri

    @uri.setter
    def uri(self, uri):
        self._uri_wrapper = mongo_uri_tools.parse_mongo_uri(uri)

    ###########################################################################
    @property
    def uri_wrapper(self):
        return self._uri_wrapper

    ###########################################################################
    def get_source_address(self, **kwargs):
        # TODO choose best member for clusters
        return self.uri

    ###########################################################################
    @property
    def database_name(self):
        if self.uri_wrapper:
            return self.uri_wrapper.database

    ###########################################################################
    def is_cluster_source(self):
        return mongo_uri_tools.is_cluster_mongo_uri(self.uri)

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "MongoSource",
            "uri": (self.uri_wrapper.masked_uri if display_only else
                    self.uri_wrapper.raw_uri)
        }

    ###########################################################################
    def validate(self):
        errors = []
        if not self.uri:
            errors.append("Missing 'uri' property")
        elif not mongo_uri_tools.is_mongo_uri(self.uri):
            errors.append("Invalid 'uri'.%s" % e)

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
    def to_document(self, display_only=False):
        return {
            "_type": "EbsVolumeSource",
            "volumeId": self.volume_id,
            "accessKey": "xxxxx" if display_only else self.access_key,
            "secretKey": "xxxxx" if display_only else self.secret_key
        }