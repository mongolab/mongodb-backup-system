__author__ = 'abdul'

# Contains mongo db utility functions
import operator

import pymongo
import pymongo.errors
import mbs_logging

from mongo_uri_tools import parse_mongo_uri
from bson.son import SON
from errors import *
from date_utils import timedelta_total_seconds
from utils import is_host_local, document_pretty_string
from verlib import NormalizedVersion, suggest_normalized_version
from bson.objectid import ObjectId

from robustify.robustify import robustify

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

# CONSTS
# db connection timeout, 160 seconds
CONN_TIMEOUT = 160

###############################################################################
@robustify(max_attempts=3, retry_interval=3,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def mongo_connect(uri, conn_timeout=None):

    # default connection timeout and convert to mills
    conn_timeout_mills = (conn_timeout or CONN_TIMEOUT) * 1000
    uri_wrapper = parse_mongo_uri(uri)

    try:
        dbname = uri_wrapper.database
        if not dbname:
            dbname = "admin"
            if uri.endswith("/"):
                uri += "admin"
            else:
                uri += "/admin"

        conn = pymongo.Connection(uri, socketTimeoutMS=conn_timeout_mills,
                                  connectTimeoutMS=conn_timeout_mills)
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
    def __init__(self, uri, conn_timeout=None):
        self._uri_wrapper = parse_mongo_uri(uri)
        self._conn_timeout = conn_timeout

    ###########################################################################
    @property
    def uri(self):
        return self._uri_wrapper.raw_uri

    ###########################################################################
    @property
    def connection(self):
        return None

    ###########################################################################
    @property
    def conn_timeout(self):
        return self._conn_timeout

    ###########################################################################
    def is_online(self):
        return self.connection is not None

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def get_mongo_version(self):
        try:
            version = self.connection.server_info()['version']
            return MongoNormalizedVersion(version)
        except Exception, e:
            if is_connection_exception(e):
                raise ConnectionError(self._uri_wrapper.masked_uri, cause=e,
                      details="Error while trying to grab mongo version "
                              "for '%s'" % self._uri_wrapper.masked_uri)
            else:
                raise



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
    @property
    def me(self):
        master_result = self._is_master_command()
        return self.address if not master_result else master_result["me"]

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def _is_master_command(self):
        return (self.is_online() and
                self.connection["admin"].command({"isMaster" : 1}))

    ###########################################################################
    def __str__(self):
        return self._uri_wrapper.masked_uri

###############################################################################
class MongoDatabase(MongoConnector):

    ###########################################################################
    def __init__(self, uri, conn_timeout=None):
        MongoConnector.__init__(self, uri, conn_timeout=conn_timeout)
        # validate that uri has a database
        if not self._uri_wrapper.database:
            raise ConfigurationError("Uri must contain a database")

        self._database = mongo_connect(uri, conn_timeout=conn_timeout)

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
            stats = _calculate_database_stats(self._database)
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
    def __init__(self, uri, conn_timeout=None):
        MongoConnector.__init__(self, uri, conn_timeout=conn_timeout)
        self._members = None
        self._primary_member = None

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
        return self.primary_member.connection

    ###########################################################################
    def _init_members(self):
        uri_wrapper = self._uri_wrapper
        # validate that uri has DB set to admin or nothing
        if uri_wrapper.database and uri_wrapper.database != "admin":
            raise ConfigurationError("Database in uri '%s' can only be admin "
                                     "or unspecified" % uri_wrapper.masked_uri)
        members = []
        primary_member = None
        for member_uri in uri_wrapper.member_raw_uri_list:
            member = MongoServer(member_uri,
                                 conn_timeout=self.conn_timeout)
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
            try:
                if not member.is_online():
                    logger.info("Member '%s' appears to be offline. "
                                "Excluding..." % member)
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
            except Exception, ex:
                logger.exception("get_best_secondary(): Cannot determine "
                                 "lag for '%s'. Skipping " % member)

        if not all_secondaries:
            logger.info("No secondaries found for cluster '%s'" % self)

        # NOTE: we use member_host property to sort instead of address since
        # a member might have multiple addresses mapped to it but member_host
        # will always be the same regardless which address you use to connect
        # to the member. This is to ensure that this algorithm produces
        # consistent results

        hidden_secondaries.sort(key=operator.attrgetter('member_host'))
        p0_secondaries.sort(key=operator.attrgetter('member_host'))
        other_secondaries.sort(key=operator.attrgetter('member_host'))

        # merge results into one list
        merged_list = hidden_secondaries + p0_secondaries + other_secondaries

        if merged_list:
            for secondary in merged_list:
                if not max_lag_seconds:
                    return secondary
                elif secondary.lag_in_seconds < max_lag_seconds:
                    return secondary

    ###########################################################################
    def has_p0s(self):
        """

        :return: True if cluster has any member with priority 0
        """
        for member in self.members:
            if member.is_online() and (member.priority == 0 or member.hidden):
                return True

        return False
    ###########################################################################
    def get_stats(self, only_for_db=None):
        return self.primary_member.get_stats(only_for_db=only_for_db)

###############################################################################
class MongoServer(MongoConnector):
###############################################################################

    ###########################################################################
    def __init__(self, uri, conn_timeout=None):
        MongoConnector.__init__(self, uri)
        self._connection = None
        self._authed_to_admin = False

        self._rs_conf = None
        self._rs_status = None
        self._member_config = None
        self._lag_in_seconds = 0

        try:
            # default connection timeout and convert to mills
            conn_timeout_mills = (conn_timeout or CONN_TIMEOUT) * 1000

            self._connection = pymongo.Connection(
                self._uri_wrapper.address,
                socketTimeoutMS=conn_timeout_mills,
                connectTimeoutMS=conn_timeout_mills)

            self._admin_db = self._connection["admin"]

            # if this is an arbiter then this is the farthest that we can get
            # to
            if self.is_arbiter():
                return

        except Exception, e:
            if is_connection_exception(e):
                logger.error("Error while trying to connect to '%s'. %s" %
                             (self, e))
                return
            else:
                raise

    ###########################################################################
    def get_auth_admin_db(self):
        if self._authed_to_admin:
            return self._admin_db

        # authenticate to admin db if creds are available
        if self._uri_wrapper.username:
            auth = self._admin_db.authenticate(self._uri_wrapper.username,
                self._uri_wrapper.password)
            if not auth:
                raise AuthenticationFailedError(self._uri_wrapper.masked_uri)

        self._authed_to_admin = True
        return self._admin_db

    ###########################################################################
    def get_db(self, name):
        return self.get_auth_admin_db().connection[name]

    ###########################################################################
    @property
    def connection(self):
        return self._connection

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
        if not self.is_arbiter() and not self._rs_conf:
            self._rs_conf = self._get_rs_config()

        return self._rs_conf

    ###########################################################################
    @property
    def member_config(self):
        if not self._member_config:
            self._member_config = self._get_member_config()

        return self._member_config

    ###########################################################################
    def member_config_prop(self, key):
        return self.member_config and self.member_config.get(key)

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
    def is_too_stale(self):
        """
            Returns true if the member is too stale
        """
        return (self.rs_status and
                "errmsg" in self.rs_status and
                "RS102" in self.rs_status["errmsg"])

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
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
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=swallow_exception)
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
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def _get_server_status(self):
        try:
            server_status_cmd = SON([('serverStatus', 1)])
            server_status = self.get_auth_admin_db().command(server_status_cmd)
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
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
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
        return self.member_config_prop("priority")

    ###########################################################################
    @property
    def hidden(self):
        return self.member_config_prop("hidden")

    ###########################################################################
    @property
    def member_host(self):
        """
            returns the "host" property from the rs member config
        """
        return self.member_config_prop("host")

    ###########################################################################
    def _get_member_config(self):
        if self.rs_conf:
            host = self.me
            mem_confs = self.rs_conf["members"]
            for mem_conf in mem_confs:
                if mem_conf["host"] == host:
                    return mem_conf

    ###########################################################################
    def fsynclock(self):
        """
            Runs fsynclock command on the server
        """

        try:
            logger.info("Attempting to run fsynclock on %s" % self)

            if self.is_server_locked():
                raise MongoLockError("Cannot run fsynclock on server '%s' "
                                     "because its already locked!" % self)
            admin_db = self.get_auth_admin_db()
            result = admin_db.command(SON([("fsync", 1),("lock", True)]))


            if result.get("ok"):
                logger.info("fsynclock ran successfully on %s" % self)
            else:
                msg = ("fsynclock was not successful on '%s'. Result: %s" %
                       document_pretty_string(result))
                raise MongoLockError(msg)
        except Exception, e:
            msg = "Error while executing fsynclock on '%s'. %s" % (self, e)
            logger.error(msg)
            if not isinstance(e, MongoLockError):
                raise MongoLockError(msg=msg, cause=e)
            else:
                raise

    ###########################################################################
    def is_server_locked(self):
        logger.info("Checking if '%s' is already locked." % self)
        admin_db = self.get_auth_admin_db()
        current_op = admin_db.current_op()
        locked = current_op and current_op.get("fsyncLock") is not None

        logger.info("is_server_locked return '%s' for '%s'." % (locked, self))
        return locked

    ###########################################################################
    def fsyncunlock(self):
        """
            Runs fsynclock command on the server
        """

        try:
            logger.info("Attempting to run fsyncunlock on %s" % self)

            admin_db = self.get_auth_admin_db()
            result = admin_db["$cmd.sys.unlock"].find_one()

            if result.get("ok"):
                logger.info("fsyncunlock ran successfully on %s" % self)
            else:
                msg = ("fsyncunlock was not successful on '%s'. Result: %s" %
                       (self, document_pretty_string(result)))
                raise MongoLockError(msg)
        except Exception, e:
            msg = "Error while executing fsyncunlock on '%s'. %s" % (self, e)
            logger.error(msg)
            if not isinstance(e, MongoLockError):
                msg = "Error while executing fsyncunlock on '%s'." % self
                raise MongoLockError(msg=msg, cause=e)
            else:
                raise

    ###########################################################################
    def get_db_path(self):
        return self.get_cmd_line_opts()["dbpath"]

    ###########################################################################
    def get_cmd_line_opts(self):
        return self._admin_db.command({"getCmdLineOpts": 1})["parsed"]

    ###########################################################################
    def is_config_server(self):
        return "configsvr" in self.get_cmd_line_opts()


###############################################################################
class ShardedClusterConnector(MongoConnector):
    ###########################################################################
    def __init__(self, uri, shard_uris, config_server_uris):
        super(ShardedClusterConnector, self).__init__(uri)

        # init routers

        routers = []
        for router_uri in self._uri_wrapper.member_raw_uri_list:
            router = MongoServer(router_uri)
            routers.append(router)

        self._routers = routers

        # Shards
        self._shards = map(lambda shard_uri: MongoCluster(shard_uri),
                           shard_uris)

        # Config Servers
        self._config_servers = map(
            lambda server_uri: MongoServer(server_uri),
            config_server_uris)

        self._selected_shard_secondaries = None
        self._selected_config_server= None


    ###########################################################################
    def is_online(self):
        return self.any_online_router() is not None

    ###########################################################################
    @property
    def routers(self):
        return self._routers

    ###########################################################################
    @property
    def shards(self):
        return self._shards

    ###########################################################################
    @property
    def config_servers(self):
        return self._config_servers

    ###########################################################################
    @property
    def selected_shard_secondaries(self):
        return self._selected_shard_secondaries

    ###########################################################################
    def any_online_router(self):
        for router in self.routers:
            if router.is_online():
                return router
        raise Exception("No online routers found for '%s'" % self)

    ###########################################################################
    @property
    def selected_config_server(self):
        if not self._selected_config_server:
            for cs in self.config_servers:
                if cs.is_online():
                    self._selected_config_server = cs

        return self._selected_config_server


    ###########################################################################
    def get_stats(self, only_for_db=None):
        stats = self.any_online_router().get_stats(only_for_db=only_for_db)
        # also capture stats from all shards
        if self.selected_shard_secondaries:
            all_shard_stats = []
            for shard_secondary in self.selected_shard_secondaries:
                shard_stats = shard_secondary.get_stats(only_for_db=
                                                        only_for_db)
                all_shard_stats.append(shard_stats)

            stats["allShardStats"] = all_shard_stats

        return stats

    ###########################################################################
    def select_shard_best_secondaries(self, max_lag_seconds=0):
        best_secondaries = []

        for shard in self.shards:
            shard_best = shard.get_best_secondary(max_lag_seconds=
                                                  max_lag_seconds)
            best_secondaries.append(shard_best)

        self._selected_shard_secondaries = best_secondaries

        return best_secondaries

    ###########################################################################
    def config_db(self):
        return self.any_online_router().get_db("config")

    ###########################################################################
    def is_balancer_active(self):
        return not(self.is_balancer_running() or self.get_balancer_state())

    ###########################################################################
    def is_balancer_running(self):
        balancer_lock = self._get_balancer_lock()
        state = balancer_lock and balancer_lock.get("state")
        return state and state > 0

    ###########################################################################
    def get_balancer_state(self):
        balancer_lock = self._get_balancer_lock()
        return balancer_lock is None or not balancer_lock.get("stopped")

    ###########################################################################
    def _get_balancer_lock(self):
        return self.config_db().locks.find_one({ "_id": "balancer" })

    ###########################################################################
    def __str__(self):
        ss = super(ShardedClusterConnector, self).__str__()
        if self.selected_shard_secondaries:
            shards_str = map(lambda s: str(s), self.selected_shard_secondaries)
            return ("%s (selected shard secondaries :%s, selected conf server "
                    "'%s')" % (ss, shards_str, self.selected_config_server))
        else:
            return ss


###############################################################################
@robustify(max_attempts=3, retry_interval=3,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def _calculate_database_stats(db):
    try:
        db_stats = db.command({"dbstats": 1})

        result = {
            "databaseName": db.name
        }

        stats_keys = [
            "collections",
            "objects",
            "dataSize",
            "storageSize",
            "indexes",
            "indexSize",
            "fileSize",
            "nsSizeMB"
        ]

        for key in stats_keys:
            result[key] = db_stats.get(key) or 0

        return result
    except pymongo.errors.OperationFailure, ofe:
        msg = ("_calculate_database_stats(): Error while trying to run"
               " dbstats for db '%s'. Cause: %s" % (db.name, ofe))
        logger.error(msg)
        raise DBStatsError(msg=msg, cause=ofe)

###############################################################################
@robustify(max_attempts=3, retry_interval=3,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def _calculate_connection_databases_stats(connection):

    all_db_stats = {}

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
        all_db_stats[dbname] = db_stats
        for key in total_stats.keys():
            total_stats[key] += db_stats.get(key) or 0

    total_stats["databaseStats"] = all_db_stats
    return total_stats


###############################################################################
def build_mongo_connector(uri):
    """
        Creates a mongo connector based on the URI passed
    """
    uri_wrapper = mongo_uri_tools.parse_mongo_uri(uri)
    if uri_wrapper.is_cluster_uri() and not uri_wrapper.database:
        return MongoCluster(uri)
    elif not uri_wrapper.database:
        return MongoServer(uri)
    else:
        return MongoDatabase(uri)

###############################################################################
def objectiditify(id):
    """
        Returns the specified id as an object id
    """
    if not isinstance(id, ObjectId):
        id = ObjectId(str(id))
    return id

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



