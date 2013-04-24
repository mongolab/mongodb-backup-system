__author__ = 'abdul'

# Contains mongo db utility functions
import pymongo
import mbs_logging

from mongo_uri_tools import parse_mongo_uri
from bson.son import SON
from errors import *
from date_utils import timedelta_total_seconds
from utils import is_host_local
from verlib import NormalizedVersion, suggest_normalized_version


###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

# CONSTS
# db connection timeout, 30 seconds
CONN_TIMEOUT = 30000

###############################################################################
def mongo_connect(uri):
    uri_wrapper = parse_mongo_uri(uri)

    try:
        dbname = uri_wrapper.database
        if not dbname:
            dbname = "admin"
            if uri.endswith("/"):
                uri += "admin"
            else:
                uri += "/admin"

        conn = pymongo.Connection(uri, socketTimeoutMS=CONN_TIMEOUT,
                                       connectTimeoutMS=CONN_TIMEOUT)
        return conn[dbname]
    except Exception, e:
        if is_connection_exception(e):
            raise ConnectionError(uri_wrapper.masked_uri, cause=e)
        elif "authentication failed" in str(e):
            raise AuthenticationFailedError(uri_wrapper.masked_uri, cause=e)
        else:
            raise

###############################################################################
class MongoConnector(object):

    ###########################################################################
    def __init__(self, uri):
        self._uri_wrapper = parse_mongo_uri(uri)

    ###########################################################################
    @property
    def uri(self):
        return self._uri_wrapper.raw_uri

    ###########################################################################
    @property
    def connection(self):
        pass

    ###########################################################################
    def get_mongo_version(self):
        return MongoNormalizedVersion(self.connection.server_info()['version'])

    ###########################################################################
    def get_stats(self, only_for_db=None):
        """
            Must be overridden
        """

    ###########################################################################
    @property
    def address(self):
        return self._uri_wrapper.addresses[0]

    ###########################################################################
    def is_local(self):
        """
            Returns true if the connector is running locally.
            Raises a ConfigurationError if called on a cluster.

        """

        if self._uri_wrapper.is_cluster_uri():
            raise ConfigurationError("Cannot call is_local() on '%s' because"
                                     " it is a cluster" % self)
        try:

            server_host = self.address.split(":")[0]
            return server_host is None or is_host_local(server_host)
        except Exception, e:
            logger.error("Unable to resolve address for server '%s'."
                         " Cause: %s" % (self, e))
        return False

    ###########################################################################
    def __str__(self):
        return self._uri_wrapper.masked_uri

###############################################################################
class MongoDatabase(MongoConnector):

    ###########################################################################
    def __init__(self, uri):
        MongoConnector.__init__(self, uri)
        # validate that uri has a database
        if not self._uri_wrapper.database:
            raise ConfigurationError("Uri must contain a database")

        self._database = mongo_connect(uri)

    ###########################################################################
    @property
    def database(self):
        return self._database

    ###########################################################################
    @property
    def connection(self):
        return self._database.connection

    ###########################################################################
    def get_stats(self, only_for_db=None):
        try:
            stats =  _calculate_database_stats(self._database)
            # capture host in stats
            conn = self._database.connection
            stats["host"] = "%s:%s" % (conn.host, conn.port)
            return stats
        except Exception, e:
            if is_connection_exception(e):
                raise ConnectionError(self._uri_wrapper.masked_uri,
                                      details="Compute database stats",
                                      cause=e)
            else:
                raise


###############################################################################
class MongoCluster(MongoConnector):
    ###########################################################################
    def __init__(self, uri):
        MongoConnector.__init__(self, uri)
        self._init_members()

    ###########################################################################
    @property
    def members(self):
        return self._members

    ###########################################################################
    @property
    def primary_member(self):
        return self._primary_member

    ###########################################################################
    @property
    def connection(self):
        raise Exception("Unsupported operation")

    ###########################################################################
    def _init_members(self):
        uri_wrapper = self._uri_wrapper
        # validate that uri has DB set to admin or nothing
        if uri_wrapper.database and uri_wrapper.database != "admin":
            raise ConfigurationError("Database in uri '%s' can only be admin or"
                                     " unspecified" % uri_wrapper.masked_uri)
        members = []
        primary_member = None
        for member_uri in uri_wrapper.member_raw_uri_list:
            member = MongoServer(member_uri)
            members.append(member)
            if member.is_online() and member.is_primary():
                primary_member = member

        if not primary_member:
            raise PrimaryNotFoundError(uri_wrapper.masked_uri)
        self._members = members
        self._primary_member = primary_member

    ###########################################################################
    def get_best_secondary(self, max_lag_seconds=0):
        """
            Returns the best source member to get the pull from.
            This only applicable for cluster connections.
            best = passives with least lags, if no passives then least lag
        """
        members = self.members

        all_secondaries = []
        hidden_secondaries = []
        p0_secondaries = []
        other_secondaries = []

        master_status = self.primary_member.rs_status

        # find secondaries
        for member in members:
            if not member.is_online():
                logger.info("Member '%s' appears to be offline. Excluding..." %
                            member)
                continue
            elif member.is_secondary():
                all_secondaries.append(member)
                # compute lags
                member.compute_lag(master_status)
                if member.hidden:
                    hidden_secondaries.append(member)
                elif member.priority == 0:
                    p0_secondaries.append(member)
                else:
                    other_secondaries.append(member)


        if not all_secondaries:
            logger.info("No secondaries found for cluster '%s'" % self)

        # sort each list by member address
        def sort_key(member):
            return member.address

        sorted(hidden_secondaries, key=sort_key)
        sorted(p0_secondaries, key=sort_key)
        sorted(other_secondaries, key=sort_key)

        # merge results into one list
        merged_list = hidden_secondaries + p0_secondaries + other_secondaries

        if merged_list:
            best_secondary = merged_list[0]
            if not max_lag_seconds:
                return best_secondary
            elif best_secondary.lag_in_seconds < max_lag_seconds:
                return best_secondary

###############################################################################
class MongoServer(MongoConnector):
###############################################################################

    ###########################################################################
    def __init__(self, uri):
        MongoConnector.__init__(self, uri)
        self._connection = None
        self._is_online = False

        try:
            self._connection = pymongo.Connection(self._uri_wrapper.address,
                                                socketTimeoutMS=CONN_TIMEOUT,
                                                connectTimeoutMS=CONN_TIMEOUT)
            self._admin_db = self._connection["admin"]
            self._authed_to_admin = False
            # connection success! set online to true
            self._is_online = True

            # if this is an arbiter then this is the farthest that we can get
            # to
            if self.is_arbiter():
                return;

        except Exception, e:
            if is_connection_exception(e):
                logger.error("Error while trying to connect to '%s'. %s" %
                             (self, e))
                return
            else:
                raise

        self._rs_conf = None
        self._rs_status = None
        self._member_config = None
        self._lag_in_seconds = 0

    ###########################################################################
    def get_auth_admin_db(self):
        if self._authed_to_admin:
            return self._admin_db

        # authenticate to admin db if creds are available
        if self._uri_wrapper.username:
            auth = self._admin_db.authenticate(self._uri_wrapper.username,
                self._uri_wrapper.password)
            if not auth:
                raise AuthenticationFailedError(self.uri_wrapper.masked_uri)

        self._authed_to_admin = True
        return self._admin_db

    ###########################################################################
    @property
    def connection(self):
        return self._connection

    ###########################################################################
    def is_online(self):
        return self._is_online

    ###########################################################################
    @property
    def lag_in_seconds(self):
        return self._lag_in_seconds

    ###########################################################################
    @property
    def optime(self):
        if self.rs_status:
            return self.rs_status['optimeDate']

    ###########################################################################
    @property
    def rs_status(self):
        if not self._rs_status and self.is_replica_member():
            self._rs_status = self._get_rs_status()

        return self._rs_status

    ###########################################################################
    @property
    def rs_conf(self):
        if not self._rs_conf:
            self._rs_conf = self._get_rs_config()

        return self._rs_conf

    ###########################################################################
    @property
    def member_config(self):
        if not self._member_config:
            self._member_config = self._get_member_config()

        return self._member_config

    ###########################################################################
    def compute_lag(self, master_status):
        """Given two 'members' elements from rs.status(),
        return lag between their optimes (in secs).
        """
        my_status = self.rs_status

        if not my_status:
            details = ("Unable to determine replicaset status for member '%s'"
                       % self)
            raise ConnectionError(self._uri_wrapper.masked_uri,
                                  details=details)

        lag_in_seconds = abs(timedelta_total_seconds(
            master_status['optimeDate'] -
            my_status['optimeDate']))

        self._lag_in_seconds = lag_in_seconds
        return self._lag_in_seconds

    ###########################################################################
    def is_primary(self):
        """
            Returns true if member is primary
        """
        master_result = self._is_master_command()
        return master_result and master_result.get("ismaster")

    ###########################################################################
    def is_secondary(self):
        """
            Returns true if the member is secondary
        """
        master_result = self._is_master_command()
        return master_result and master_result.get("secondary")

    ###########################################################################
    def is_arbiter(self):
        """
            Returns true if the member is an arbiter
        """
        master_result = self._is_master_command()
        return master_result and master_result.get("arbiterOnly")

    ###########################################################################
    def is_replica_member(self):
        """
            Returns true if this is a replica member
        """

        return self.get_replicaset_name() is not None

    ###########################################################################
    def get_replicaset_name(self):
        """
            Returns true if the member is secondary
        """
        master_result = self._is_master_command()
        return master_result and master_result.get("setName")

    ###########################################################################
    def _is_master_command(self):
        return self._admin_db.command({"isMaster" : 1})

    ###########################################################################
    def is_too_stale(self):
        """
            Returns true if the member is too stale
        """
        return (self.rs_status and
                "errmsg" in self.rs_status and
                "RS102" in self.rs_status["errmsg"])

    ###########################################################################
    def get_stats(self, only_for_db=None):

        # ensure that we are authed to admin
        self.get_auth_admin_db()

        # compute database stats
        try:
            if only_for_db:
                db = self._connection[only_for_db]
                db_stats = _calculate_database_stats(db)
            else:
                conn = self._connection
                db_stats = _calculate_connection_databases_stats(conn)


            stats =  {
                "optime": self.optime,
                "replLagInSeconds": self.lag_in_seconds

            }
            stats.update(db_stats)
            stats.update(self._get_server_status())
            return stats
        except Exception, e:
            if is_connection_exception(e):
                details = ("Error while trying to compute stats for server "
                           "'%s'." % self)
                raise ConnectionError(self._uri_wrapper.masked_uri,
                                      details=details, cause=e)
            else:
                raise

    ###########################################################################
    def _get_rs_status(self):
        try:
            rs_status_cmd = SON([('replSetGetStatus', 1)])
            rs_status =  self.get_auth_admin_db().command(rs_status_cmd)
            for member in rs_status['members']:
                if 'self' in member and member['self']:
                    return member
        except Exception, e:
            details = "Cannot get rs for member '%s'" % self
            raise ReplicasetError(details=details, cause=e)

    ###########################################################################
    def _get_server_status(self):
        try:
            server_status_cmd = SON([('serverStatus', 1)])
            server_status =  self.get_auth_admin_db().command(server_status_cmd)
            ignored_props = ["locks", "recordStats"]
            # IMPORTANT NOTE: We remove the "locks" property
            # which is introduced in 2.2.0 to avoid having issues if a client
            # tries to save the returned document. this is because "locks"
            # contain a key "." which is not allowed by mongodb. Also "locks"
            # Could be very big and is not really needed anyways...
            for prop in ignored_props:
                if prop in server_status:
                    del server_status[prop]
            return server_status
        except Exception, e:
            details = "Cannot get server status for member '%s'. " % self
            raise ServerError(self._uri_wrapper.masked_uri, details=details,
                              cause=e)

    ###########################################################################
    def _get_rs_config(self):

        try:
            local_db = self.get_auth_admin_db().connection["local"]
            return local_db['system.replset'].find_one()
        except Exception, e:
                details = "Cannot get rs config for member '%s'." % self
                raise ReplicasetError(details=details, cause=e)


    ###########################################################################
    @property
    def priority(self):
        return self.member_config.get("priority")

    ###########################################################################
    @property
    def hidden(self):
        return self.member_config.get("hidden")

    ###########################################################################
    def _get_member_config(self):
        if self.rs_conf:
            host = self.address
            mem_confs = self.rs_conf["members"]
            for mem_conf in mem_confs:
                if mem_conf["host"] == host:
                    return mem_conf

###############################################################################
def database_connection_stats(db_uri):
    """
        Returns database stats for the specified database uri
    """
    db = mongo_connect(db_uri)
    return _calculate_database_stats(db)

###############################################################################
def _calculate_database_stats(db):
    db_stats = db.command({"dbstats":1})

    result = {
        "collections": db_stats["collections"],
        "objects": db_stats["objects"],
        "dataSize": db_stats["dataSize"],
        "storageSize": db_stats["storageSize"],
        "indexes": db_stats["indexes"],
        "indexSize":db_stats["indexSize"],
        "fileSize": db_stats["fileSize"],
        "nsSizeMB": db_stats["nsSizeMB"],
        "databaseName": db.name
    }

    return result

###############################################################################
def _calculate_connection_databases_stats(connection):

    total_stats = {
        "collections": 0,
        "objects": 0,
        "dataSize": 0,
        "storageSize": 0,
        "indexes": 0,
        "indexSize": 0,
        "fileSize": 0,
        "nsSizeMB": 0
    }


    database_names = connection.database_names()

    for dbname in database_names:
        if dbname == "local":
            continue

        db = connection[dbname]
        db_stats = _calculate_database_stats(db)
        for key in total_stats.keys():
            total_stats[key] += db_stats.get(key) or 0

    return total_stats



###############################################################################
# MongoNormalizedVersion class
# we had to inherit and override __str__ because the suggest_normalized_version
# method does not maintain the release candidate version properly
###############################################################################
class MongoNormalizedVersion(NormalizedVersion):
    def __init__(self, version_str):
        sugg_ver = suggest_normalized_version(version_str)
        super(MongoNormalizedVersion,self).__init__(sugg_ver)
        self.version_str = version_str

    def __str__(self):
        return self.version_str



