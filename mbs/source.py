__author__ = 'abdul'

from utils import document_pretty_string, is_cluster_mongo_uri, parse_mongo_uri

from boto.ec2.connection import EC2Connection


###############################################################################
# Backup Source Classes
###############################################################################
class BackupSource(object):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    @property
    def source_uri(self):
        pass

    ###########################################################################
    @property
    def username(self):
        pass

    ###########################################################################
    @property
    def password(self):
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

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())

###############################################################################
# ServerSource
###############################################################################
class ServerSource(BackupSource):

    ###########################################################################
    def __init__(self):
        BackupSource.__init__(self)
        self._address = None
        self._admin_username = None
        self._admin_password = None

    ###########################################################################
    @property
    def address(self):
        return self._address

    @address.setter
    def address(self, address):
        self._address = address

    ###########################################################################
    @property
    def username(self):
        return self.admin_username

    ###########################################################################
    @property
    def password(self):
        return self.admin_password

    ###########################################################################
    @property
    def admin_username(self):
        return self._admin_username

    @admin_username.setter
    def admin_username(self, admin_username):
        self._admin_username = admin_username

    ###########################################################################
    @property
    def admin_password(self):
        return self._admin_password

    @admin_password.setter
    def admin_password(self, admin_password):
        self._admin_password = admin_password

    ###########################################################################
    @property
    def source_uri(self):
        return "mongodb://%s" % self.address

    ###########################################################################
    def to_document(self):
        return {
            "_type": "ServerSource",
            "address": self.address,
            "adminUsername": self.admin_username,
            "adminPassword": self.admin_password
        }

    ###########################################################################
    def validate(self):
        errors = []
        if not self.address:
            errors.append("Missing 'address' property")

        if not self.admin_username:
            errors.append("Missing 'adminUsername' property")

        if not self.password:
            errors.append("Missing 'password' property")

        return errors

###############################################################################
# Database Source
###############################################################################
class DatabaseSource(BackupSource):

    ###########################################################################
    def __init__(self):
        BackupSource.__init__(self)
        self._database_uri = None

    ###########################################################################
    @property
    def database_uri(self):
        return self._database_uri

    @database_uri.setter
    def  database_uri(self, database_uri):
        self._database_uri = database_uri

    ###########################################################################
    @property
    def source_uri(self):
        return self.database_uri

    ###########################################################################
    def to_document(self):
        return {
            "_type": "DatabaseSource",
            "databaseUri": self.database_uri
        }

    ###########################################################################
    def is_cluster_source(self):
        return is_cluster_mongo_uri(self.database_uri)


    ###########################################################################
    def validate(self):
        errors = []
        if not self.database_uri:
            errors.append("Missing 'databaseUri' property")

        try:
            parse_mongo_uri(self.database_uri)
        except Exception, e:
            errors.append("Invalid 'databaseUri'.%s" % e)

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