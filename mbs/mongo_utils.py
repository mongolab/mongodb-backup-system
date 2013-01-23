__author__ = 'abdul'

# Contains mongo db utility functions
import pymongo
import mbs_logging

from mongo_uri_tools import parse_mongo_uri
from bson.son import SON
from errors import *
from date_utils import timedelta_total_seconds


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
            raise ConnectionError("Could not establish a database connection "
                                  "to %s" % uri_wrapper.masked_uri, cause=e)
        else:
            raise

###############################################################################
class MongoDatabase(object):

    ###########################################################################
    def __init__(self, uri):
        self._uri_wrapper = parse_mongo_uri(uri)
        # validate that uri has a database
        if not self._uri_wrapper.database:
            raise ConfigurationError("Uri must contain a database")

        self._connection = mongo_connect(uri)
        self._database = self._connection[self._uri_wrapper.database]
        self._database_stats = _calculate_database_stats(self._database)

    ###########################################################################
    @property
    def database(self):
        return self._database

    def get_stats(self):
        return self._database_stats

###############################################################################
class MongoCluster(object):
    ###########################################################################
    def __init__(self, uri):
        self._uri_wrapper = parse_mongo_uri(uri)
        self._init_members()

    ###########################################################################
    @property
    def members(self):
        return self._members

    ###########################################################################
    @property
    def uri(self):
        return self._uri_wrapper.raw_uri

    ###########################################################################
    @property
    def primary_member(self):
        return self._primary_member

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
            raise PrimaryNotFoundError("Unable to determine primary for "
                                       "cluster '%s'" % self)
        self._members = members
        self._primary_member = primary_member

    ###########################################################################
    def get_best_secondary(self, min_lag_seconds=0):
        """
            Returns the best source member to get the pull from.
            This only applicable for cluster connections.
            best = passives with least lags, if no passives then least lag
        """
        members = self.members
        secondaries = []
        # find secondaries
        for member in members:
            if not member.is_online():
                logger.info("Member '%s' appears to be offline. Excluding..." %
                            member)
                continue
            elif member.is_secondary():
                secondaries.append(member)


        if not secondaries:
            logger.info("No secondaries found for cluster '%s'" % self)

        master_status = self.primary_member.rs_status
        # compute lags
        for secondary in secondaries:
            secondary.compute_lag(master_status)

        def best_secondary_comp(member1, member2):

            if member1.is_passive():
                if member2.is_passive():
                    return int(member1.lag_in_seconds - member2.lag_in_seconds)
                else:
                    return -1
            elif member2.is_passive():
                return 1
            else:
                return int(member1.lag_in_seconds - member2.lag_in_seconds)


        secondaries.sort(best_secondary_comp)
        if secondaries:
            best_secondary = secondaries[0]
            if (min_lag_seconds and
                best_secondary.lag_in_seconds > min_lag_seconds):
                return None
            else:
                return best_secondary

    ###########################################################################
    def __str__(self):
        return self._uri_wrapper.masked_uri

###############################################################################
class MongoServer(object):
###############################################################################

    ###########################################################################
    def __init__(self, uri):
        self._uri_wrapper = parse_mongo_uri(uri)
        self._admin_db = None
        self._is_online = False

        try:
            self._admin_db = mongo_connect(uri)
            # connection success! set online to true
            self._is_online = True
        except Exception, e:
            logger.error("Error while trying to connect to '%s'. %s" %
                         (self, e))
            return
        self._rs_conf = self._get_rs_config()
        self._rs_status = self._get_rs_status()
        self._member_config = self._get_member_config()
        self._lag_in_seconds = 0

    ###########################################################################
    @property
    def uri(self):
        return self._uri_wrapper.raw_uri

    ###########################################################################
    def is_online(self):
        return self._is_online

    ###########################################################################
    @property
    def address(self):
        return self._uri_wrapper.addresses[0]

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
        return self._rs_status

    ###########################################################################
    def compute_lag(self, master_status):
        """Given two 'members' elements from rs.status(),
        return lag between their optimes (in secs).
        """
        my_status = self._rs_status

        if not my_status:
            raise ConnectionError("Unable to determine replicaset status for"
                                  " member '%s'" % self)

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
        master_result = self._admin_db.command({"isMaster" : 1})
        return master_result and master_result.get("ismaster")

    ###########################################################################
    def is_secondary(self):
        """
            Returns true if the member is secondary or is recovering
        """
        master_result = self._admin_db.command({"isMaster" : 1})
        return master_result and master_result.get("secondary")

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
        # compute database stats
        if only_for_db:
            db = self._admin_db.connection[only_for_db]
            db_stats = _calculate_database_stats(db)
        else:
            conn = self._admin_db.connection
            db_stats = _calculate_connection_databases_stats(conn)


        stats =  {
            "optime": self.optime,
            "replLagInSeconds": self.lag_in_seconds

        }
        stats.update(db_stats)
        stats.update(self._get_server_status())
        return stats

    ###########################################################################
    def _get_rs_status(self):
        try:
            rs_status_cmd = SON([('replSetGetStatus', 1)])
            rs_status =  self._admin_db.command(rs_status_cmd)
            for member in rs_status['members']:
                if 'self' in member and member['self']:
                    return member
        except Exception, e:
            raise ReplicasetError("Cannot get rs for member '%s'", cause=e)

    ###########################################################################
    def _get_server_status(self):
        try:
            server_status_cmd = SON([('serverStatus', 1)])
            server_status =  self._admin_db.command(server_status_cmd)

            # IMPORTANT NOTE: We remove the "locks" property
            # which is introduced in 2.2.0 to avoid having issues if a client
            # tries to save the returned document. this is because "locks"
            # contain a key "." which is not allowed by mongodb. Also "locks"
            # Could be very big and is not really needed anyways...
            if "locks" in server_status:
                del server_status["locks"]
            return server_status
        except Exception, e:
            raise ServerError("Cannot get server status for member '%s'. " %
                              self, cause=e)

    ###########################################################################
    def _get_rs_config(self):

        try:
            local_db = self._admin_db.connection["local"]
            return local_db['system.replset'].find_one()
        except Exception, e:
                raise ReplicasetError("Cannot get rs config for member '%s'." %
                                      self, cause=e)


    ###########################################################################
    def is_passive(self):
        return self._member_config.get("priority") == 0

    ###########################################################################
    def _get_member_config(self):
        if self._rs_conf:
            host = self.address
            mem_confs = self._rs_conf["members"]
            for mem_conf in mem_confs:
                if mem_conf["host"] == host:
                    return mem_conf

    ###########################################################################
    def __str__(self):
        return self.address

###############################################################################
def database_connection_stats(db_uri):
    """
        Returns database stats for the specified database uri
    """
    db = mongo_connect(db_uri)
    return _calculate_database_stats(db)

###############################################################################
def _calculate_database_stats(db):
    try:
        db_stats = db.command({"dbstats":1})

        result = {
            "collections": db_stats["collections"],
            "objects": db_stats["objects"],
            "dataSize": db_stats["dataSize"],
            "storageSize": db_stats["storageSize"],
            "indexes": db_stats["indexes"],
            "indexSize":db_stats["indexSize"],
            "fileSize": db_stats["fileSize"],
            "nsSizeMB": db_stats["nsSizeMB"]
        }

        return result
    except Exception, e:
        if is_connection_exception(e):
            raise ConnectionError("Error while trying to compute stats "
                                  "for database '%s'." % db.name, cause=e)
        else:
            raise

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
        db = connection[dbname]
        db_stats = _calculate_database_stats(db)
        for key in total_stats.keys():
            total_stats[key] += db_stats.get(key) or 0

    return total_stats
