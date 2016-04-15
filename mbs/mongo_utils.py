__author__ = 'abdul'

# Contains mongo db utility functions

import time

import pymongo
import pymongo.errors
from pymongo.mongo_client import MongoClient
from threading import Thread

from mongo_uri_tools import parse_mongo_uri
from bson.son import SON
from errors import *
from date_utils import timedelta_total_seconds
from utils import is_host_local, document_pretty_string, safe_stringify
from verlib import NormalizedVersion, suggest_normalized_version
from bson.objectid import ObjectId
import logging
from robustify.robustify import robustify
from date_utils import date_now

###############################################################################
# LOGGER
###############################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# CONSTS
# db connection timeout, 160 seconds
CONN_TIMEOUT = 160

###############################################################################
@robustify(max_attempts=3, retry_interval=3,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def mongo_connect(uri, conn_timeout=None, **kwargs):
    conn_timeout_mills = (conn_timeout or CONN_TIMEOUT) * 1000
    kwargs = kwargs or {}
    kwargs["socketTimeoutMS"] = conn_timeout_mills
    kwargs["connectTimeoutMS"] = conn_timeout_mills
    # default connection timeout and convert to mills

    uri_wrapper = parse_mongo_uri(uri)

    try:
        dbname = uri_wrapper.database
        if not dbname:
            if uri.endswith("/"):
                uri += "admin"
            else:
                uri += "/admin"

        # add serverSelectionTimeoutMS for pymongo 3.2
        if pymongo.get_version_string().startswith("3.2"):
            kwargs["serverSelectionTimeoutMS"] = 3000

        mongo_client = MongoClient(uri, **kwargs)
        # ensure connect
        ping(mongo_client)

        return mongo_client

    except Exception, e:
        if is_connection_exception(e):
            raise ConnectionError(uri_wrapper.masked_uri, cause=e)
        elif "authentication failed" in safe_stringify(e):
            raise AuthenticationFailedError(uri_wrapper.masked_uri, cause=e)
        else:
            raise

###############################################################################
def  get_client_connection_id(mongo_client):
    myuri = mongo_client.admin.command({"whatsmyuri": 1})
    if myuri:
        return myuri["you"].split(":")[1]

###############################################################################
class MongoConnector(object):

    ###########################################################################
    def __init__(self, uri, connector_id=None,display_name=None, conn_timeout=None):
        self._uri_wrapper = parse_mongo_uri(uri)
        self._connector_id = connector_id
        self._conn_timeout = conn_timeout or CONN_TIMEOUT
        self._connection_id = None
        self._display_name = display_name

    ###########################################################################
    @property
    def uri(self):
        return self._uri_wrapper.raw_uri

    ###########################################################################
    @property
    def connector_id(self):
        return self._connector_id

    ###########################################################################
    @property
    def display_name(self):
        return self._display_name

    ###########################################################################
    @property
    def mongo_client(self):
        return None

    ###########################################################################
    @property
    def admin_db(self):
        return self.get_db("admin")

    ###########################################################################
    def get_db(self, name):
        return self.mongo_client[name]

    ###########################################################################
    @property
    def connection_id(self):
        try:
            if self.is_online() and not self._connection_id:
                myuri = self.whatsmyuri()
                self._connection_id = myuri["you"].split(":")[1]
        except Exception, e:
            logger.exception("Error while determining connection id for"
                             " connector '%s'. %s" % (self, e))

        return self._connection_id

    ###########################################################################
    @property
    def conn_timeout(self):
        return self._conn_timeout

    ###########################################################################
    def is_online(self):

            try:
                if self.mongo_client:
                    ping(self.mongo_client)
                    return True
            except (pymongo.errors.OperationFailure, pymongo.errors.AutoReconnect), ofe:
                return "refused" not in str(ofe)
            except pymongo.errors.ConnectionFailure, cfe:
                return "connection closed" in str(cfe)




    ###########################################################################
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def get_mongo_version(self):
        try:
            version = self.mongo_client.server_info()['version']
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
    def get_collection_counts(self, only_for_db=None):
        """
            Must be overridden
        """

    ###########################################################################
    def whatsmyuri(self):
        pass

    ###########################################################################
    @property
    def address(self):
        return self._uri_wrapper.addresses[0]

    ###########################################################################
    @property
    def host(self):
        return self.address.split(":")[0]

    ###########################################################################
    @property
    def port(self):
        return self.address.split(":")[1]

    ###########################################################################
    @property
    def local_address(self):
        return "localhost:%s" % self.port

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

            server_host = self.host
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
                self.admin_db.command({"isMaster" : 1}))

    ###########################################################################
    def info(self):
        return "uri: '%s', connection id: %s" % (str(self), self.connection_id)

    ###########################################################################
    def __str__(self):
        return self.display_str

    ###########################################################################
    @property
    def display_str(self):
        return self.display_name or self._uri_wrapper.masked_uri

###############################################################################
class MongoDatabase(MongoConnector):

    ###########################################################################
    def __init__(self, uri, connector_id=None, display_name=None, conn_timeout=None):
        MongoConnector.__init__(self, uri, connector_id=connector_id,
                                display_name=display_name,
                                conn_timeout=conn_timeout)
        # validate that uri has a database
        if not self._uri_wrapper.database:
            raise ConfigurationError("Uri must contain a database")

        self._mongo_client = None

    ###########################################################################
    @property
    def database(self):
        return self.mongo_client.get_default_database()

    ###########################################################################
    @property
    def mongo_client(self):
        if not self._mongo_client:
            self._mongo_client = mongo_connect(self.uri, conn_timeout=self.conn_timeout)

        return self._mongo_client

    ###########################################################################
    def whatsmyuri(self):
        return self.database.command({"whatsmyuri": 1})

    ###########################################################################
    def get_stats(self, only_for_db=None):
        try:
            stats = _calculate_database_stats(self.database)
            # capture host in stats
            client = self.mongo_client
            stats["host"] = client.address
            stats["connectionId"] = self.connection_id
            stats["version"] = str(self.get_mongo_version())
            return stats
        except Exception, e:
            if is_connection_exception(e):
                raise ConnectionError(self._uri_wrapper.masked_uri,
                                      details="Compute database stats",
                                      cause=e)
            else:
                raise


    ###########################################################################
    def get_collection_counts(self, only_for_db=None):
        try:

            return {
                self.database.name: _database_collection_counts(self.database)
            }

        except Exception, e:
            if is_connection_exception(e):
                raise ConnectionError(self._uri_wrapper.masked_uri,
                                      details="get_collection_counts()",
                                      cause=e)
            else:
                raise


###############################################################################
class MongoCluster(MongoConnector):
    ###########################################################################
    def __init__(self, uri, connector_id=None, display_name=None, members=None,
                 conn_timeout=None):
        """

        :param uri: cluster uri
        :param display_name: cluster display name (not required)
        :param members: allow passing members as apposed to computing them
        from uri
        :param conn_timeout:
        :return:
        """
        MongoConnector.__init__(self, uri, connector_id=connector_id,
                                display_name=display_name,
                                conn_timeout=conn_timeout)
        self._members = members
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
    def mongo_client(self):
        return self.primary_member.mongo_client

    ####################################################################################################################
    def _init_members(self):
        uri_wrapper = self._uri_wrapper
        # validate that uri has DB set to admin or nothing
        if uri_wrapper.database and uri_wrapper.database != "admin":
            raise ConfigurationError("Database in uri '%s' can only be admin "
                                     "or unspecified" % uri_wrapper.masked_uri)
        primary_member = None
        member_uris = uri_wrapper.member_raw_uri_list

        if not self._members:
            self._members = []
            for member_uri in member_uris:
                member = MongoServer(member_uri,
                                     conn_timeout=self.conn_timeout)
                self._members.append(member)

        # find primary
        for member in self._members:
            if member.is_online() and member.is_primary():
                primary_member = member

        if not primary_member:
            raise PrimaryNotFoundError(uri_wrapper.masked_uri)

        self._primary_member = primary_member

    ###########################################################################
    def get_mongolab_backup_node(self):
        logger.info("Attempting to determine mongolab backup node for %s" % self.connector_id)
        rs_conf = self.primary_member.rs_conf
        logger.info("rs.conf for primary  '%s' is %s" % (self.primary_member.connector_id, rs_conf))
        for mem_conf in rs_conf["members"]:
            if("tags" in mem_conf and
                       "mongolabBackupNode" in mem_conf["tags"]):
                return self.get_member_by_address(mem_conf["host"])

    ###########################################################################
    def get_member_by_address(self, address):
        for member in self.members:
            if member.address == address:
                return member

    ###########################################################################
    def get_stats(self, only_for_db=None):
        return self.primary_member.get_stats(only_for_db=only_for_db)

    ###########################################################################
    def get_collection_counts(self, only_for_db=None):
        return self.primary_member.get_collection_counts(only_for_db=only_for_db)

    ###########################################################################
    def whatsmyuri(self):
        return self.primary_member.whatsmyuri()

###############################################################################
class MongoServer(MongoConnector):
###############################################################################

    ###########################################################################
    def __init__(self, uri,
                 connector_id=None,
                 display_name=None,
                 conn_timeout=None,
                 allow_local_connections=False):
        MongoConnector.__init__(self, uri, connector_id=connector_id,
                                display_name=display_name,
                                conn_timeout=conn_timeout)
        self._mongo_client = None
        self._attempted_connection = False

        self._rs_conf = None
        self._member_rs_status = None
        self._member_config = None
        self._lag_in_seconds = 0
        self._allow_local_connections = allow_local_connections

    ###########################################################################
    @property
    def connection_address(self):
        if self._allow_local_connections and self.is_local():
            return self.local_address
        else:
            return self.address

    ###########################################################################
    @property
    def mongo_client(self):
        if not self._mongo_client:
            # default connection timeout and convert to mills
            conn_timeout_mills = self.conn_timeout * 1000
            kwargs = {
                "socketTimeoutMS": conn_timeout_mills,
                "connectTimeoutMS": conn_timeout_mills
            }
            # add slaveOk for older pymongo versions
            if pymongo.get_version_string().startswith("2"):
                kwargs["slaveOk"] = True
            self._mongo_client = mongo_connect(self.uri, **kwargs)
        return self._mongo_client

    ###########################################################################
    @property
    def lag_in_seconds(self):
        return self._lag_in_seconds

    ###########################################################################
    @property
    def optime(self):
        if self.member_rs_status:
            return self.member_rs_status['optimeDate']

    ###########################################################################
    @property
    def member_rs_status(self):
        if not self._member_rs_status and self.is_replica_member():
            self._member_rs_status = self._get_member_rs_status()

        return self._member_rs_status

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
        my_status = self.member_rs_status

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
        return (self.member_rs_status and
                "errmsg" in self.member_rs_status and
                "RS102" in self.member_rs_status["errmsg"])

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def get_stats(self, only_for_db=None):

        client = self.mongo_client
        # compute database stats
        try:
            if only_for_db:
                db_stats = _calculate_database_stats(client[only_for_db])
            else:
                db_stats = _calculate_client_databases_stats(client)


            stats =  {
                "optime": self.optime,
                "replLagInSeconds": self.lag_in_seconds

            }
            stats.update(db_stats)
            stats.update(self._get_server_status())
            stats["connectionId"] = self.connection_id
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
               do_on_failure=raise_exception)
    def get_collection_counts(self, only_for_db=None):


        # compute database stats
        try:
            if only_for_db:
                db = self.mongo_client[only_for_db]
                db_col_count = _database_collection_counts(db)
                return {
                    db.name: db_col_count
                }
            else:
                return _client_collection_counts(self.mongo_client)
        except Exception, e:
            if is_connection_exception(e):
                details = ("Error while trying to compute collection counts for server "
                           "'%s'." % self)
                raise ConnectionError(self._uri_wrapper.masked_uri,
                                      details=details, cause=e)
            else:
                raise

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=swallow_exception)
    def _get_member_rs_status(self):
        try:
            rs_status_cmd = SON([('replSetGetStatus', 1)])
            rs_status = self.admin_db.command(rs_status_cmd)
            for member in rs_status['members']:
                if 'self' in member and member['self']:
                    return member
        except Exception, e:
            details = "Cannot get rs for member '%s'" % self
            raise ReplicasetError(details=details, cause=e)


    ###########################################################################
    @robustify(max_attempts=3, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=swallow_exception)
    def get_rs_status(self):
        try:
            rs_status_cmd = SON([('replSetGetStatus', 1)])
            return self.admin_db.command(rs_status_cmd)
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
            server_status = self.admin_db.command(server_status_cmd)
            ignored_props = ["locks", "recordStats", "$gleStats", "wiredTiger"]
            # IMPORTANT NOTE: We remove the "locks" property
            # which is introduced in 2.2.0 to avoid having issues if a client
            # tries to save the returned document. this is because "locks"
            # contain a key "." which is not allowed by mongodb. Also "locks"
            # Could be very big and is not really needed anyways...
            for prop in server_status.keys():
                if prop in ignored_props or prop.startswith("$"):
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
            local_db = self.mongo_client.local
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
    def slave_delay(self):
        return self.member_config_prop("slaveDelay")

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
                raise ServerAlreadyLockedError("Cannot run fsynclock on server '%s' "
                                               "because its already locked!" % self)
            result = self.admin_db.command(SON([("fsync", 1),("lock", True)]))


            if result.get("ok"):
                logger.info("fsynclock ran successfully on %s" % self)
            else:
                msg = ("fsynclock was not successful on '%s'. Result: %s" %
                       document_pretty_string(result))
                raise MongoLockError(msg)
        except Exception, e:
            msg = "Error while executing fsynclock on '%s'. %s" % (self, e)
            logger.error(msg)
            raise

    ###########################################################################
    def is_server_locked(self):
        logger.info("Checking if '%s' is already locked." % self)
        current_op = self.admin_db.current_op()
        locked = current_op and current_op.get("fsyncLock") is not None

        logger.info("is_server_locked return '%s' for '%s'." % (locked, self))
        return locked

    ###########################################################################
    @robustify(max_attempts=5, retry_interval=3,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def fsyncunlock(self):
        """
            Runs fsynclock command on the server
        """

        try:
            logger.info("Attempting to run fsyncunlock on %s" % self)

            result = self.admin_db["$cmd.sys.unlock"].find_one()

            if result.get("ok"):
                logger.info("fsyncunlock ran successfully on %s" % self)
            else:
                msg = ("fsyncunlock was not successful on '%s'. Result: %s" %
                       (self, document_pretty_string(result)))
                raise MongoLockError(msg)
        except Exception, e:
            msg = "Error while executing fsyncunlock on '%s'. %s" % (self, e)
            logger.error(msg)
            raise

    ###########################################################################
    def get_db_path(self):
        return self.get_cmd_line_opts()["dbpath"]

    ###########################################################################
    def get_cmd_line_opts(self):
        return self.admin_db.command({"getCmdLineOpts": 1})["parsed"]

    ###########################################################################
    def is_config_server(self):
        return "configsvr" in self.get_cmd_line_opts()

    ###########################################################################
    def whatsmyuri(self):
        return self.admin_db.command({"whatsmyuri": 1})

###############################################################################
class ShardedClusterConnector(MongoConnector):
    ###########################################################################
    def __init__(self, uri, routers, shards, config_servers,
                 connector_id=None, display_name=None):
        super(ShardedClusterConnector, self).__init__(uri, connector_id=connector_id,
                                                      display_name=display_name)

        self._routers = routers
        self._router = None

        # Shards
        self._shards = shards

        # Config Server
        self._config_server = None
        self._config_servers = config_servers

        self._selected_shard_secondaries = None

        # balancer activity monitor
        self._balancer_activity_monitor = Thread(target=
                                                 self._do_monitor_activity)
        self._balancer_active_during_monitor = None

        self._stop_balancer_monitor_request = True


    ###########################################################################
    def is_online(self):
        return self.router.is_online()

    ###########################################################################
    @property
    def shards(self):
        return self._shards

    ###########################################################################
    @property
    def config_server(self):
        if not self._config_server:
            for conf_server in self._config_servers:
                if conf_server.is_online():
                    self._config_server = conf_server

        if self._config_server is None:
            raise Exception("No online config servers found for '%s'" % self)
        else:
            return self._config_server

    ###########################################################################
    @property
    def selected_shard_secondaries(self):
        return self._selected_shard_secondaries

    ###########################################################################
    @property
    def router(self):
        if self._router is None:
            for router in self._routers:
                if router.is_online():
                    self._router = router

        if self._router is None:
            raise Exception("No online routers found for '%s'" % self)
        else:
            return self._router


    ###########################################################################
    def get_stats(self, only_for_db=None):
        stats = self.router.get_stats(only_for_db=only_for_db)
        # also capture stats from all shards
        if self.selected_shard_secondaries:
            all_shard_stats = []
            for shard_secondary in self.selected_shard_secondaries:
                shard_stats = shard_secondary.get_stats(only_for_db=
                                                        only_for_db)
                all_shard_stats.append(shard_stats)

            stats["shardStats"] = all_shard_stats

        return stats

    ###########################################################################
    def config_db(self):
        return self.router.get_db("config")

    ###########################################################################
    def is_balancer_active(self):
        return self._is_balancer_running() or self._get_balancer_state()

    ###########################################################################
    def _is_balancer_running(self):
        balancer_lock = self._get_balancer_lock()
        state = balancer_lock and balancer_lock.get("state")
        return state and state > 0

    ###########################################################################
    def _get_balancer_state(self):
        balancer_settings= self._get_balancer_settings()
        return (balancer_settings is None or
                not balancer_settings.get("stopped"))

    ###########################################################################
    def stop_balancer(self):
        self._set_balancer_state(False)

    ###########################################################################
    def resume_balancer(self):
        self._set_balancer_state(True)

    ###########################################################################
    def _set_balancer_state(self, val):
        self.config_db().settings.update(
            {"_id": "balancer"},
            {"$set" : { "stopped": not val}},
            True
        )

    ###########################################################################
    def _get_balancer_lock(self):
        return self.config_db().locks.find_one({"_id": "balancer"})

    ###########################################################################
    def _get_balancer_settings(self):
        return self.config_db().settings.find_one({"_id": "balancer"})


    ###########################################################################
    def start_balancer_activity_monitor(self):
        logger.info("Starting balancer activity monitor for '%s'" % self)
        self._balancer_activity_monitor.start()

    ###########################################################################
    def stop_balancer_activity_monitor(self):
        logger.info("Stopping balancer activity monitor for '%s'..." % self)
        self._stop_balancer_monitor_request = True
        self._balancer_activity_monitor.join()
        logger.info("Balancer activity monitor stopped for '%s'. Monitor detected "
                    "balancer active during: %s" %
                    (self, self.balancer_active_during_monitor()))

    ###########################################################################
    def balancer_active_during_monitor(self):
        return self._balancer_active_during_monitor

    ###########################################################################
    def _do_monitor_activity(self):
        self._balancer_active_during_monitor = None
        self._stop_balancer_monitor_request = None

        while not (self._stop_balancer_monitor_request or
                       self._balancer_active_during_monitor):
            self._balancer_active_during_monitor = self.is_balancer_active()
            if self._balancer_active_during_monitor:
                logger.info("Balancer activity monitor for '%s' detected balancer was active at '%s'" %
                            (self, date_now()))
            time.sleep(1)

    ###########################################################################
    def info(self):
        i = {
            "router": self.router.info(),
            "configServer": self.config_server.info()
        }

        if self.selected_shard_secondaries:
            shard_infos = map(lambda s: s.info(),
                              self.selected_shard_secondaries)
            i["selectedShardSecondaries"] = shard_infos

        return document_pretty_string(i)


    ###########################################################################
    def whatsmyuri(self):
        return self.router.whatsmyuri()

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
def _calculate_client_databases_stats(mongo_client):
    """

    :param mongo_client:
    :return: dict with following structure
        { [sum of all database stats except for local],
          "databaseStats": {dbname : stats} , except local
          localDatabaseStats: stats for local db
        }
    """

    all_db_stats = {}
    local_db_stats = None

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

    database_names = mongo_client.database_names()

    for dbname in database_names:
        db = mongo_client[dbname]
        db_stats = _calculate_database_stats(db)
        # capture local database stats
        if dbname == "local":
            local_db_stats = db_stats
        else:
            all_db_stats[dbname] = db_stats
            for key in total_stats.keys():
                total_stats[key] += db_stats.get(key) or 0

    total_stats["databaseStats"] = all_db_stats
    total_stats["localDatabaseStats"] = local_db_stats
    return total_stats


###############################################################################
@robustify(max_attempts=3, retry_interval=3,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def _database_collection_counts(db):
    result = []
    for cname in db.collection_names():
        # skip system collections
        if cname.startswith("system."):
            continue

        collstats = db.command("collstats", cname)
        result.append({
            "name": cname,
            "count": collstats["count"]
        })

    return result



###############################################################################
@robustify(max_attempts=3, retry_interval=3,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def _client_collection_counts(mongo_client):
    """

    :param mongo_client:
    :return: dict with all dbs collection counts

    """

    collection_counts = {}


    database_names = mongo_client.database_names()

    for dbname in database_names:
        db = mongo_client[dbname]
        db_collection_counts = _database_collection_counts(db)
        collection_counts[dbname] = db_collection_counts

    return collection_counts


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


###############################################################################
def ping(mongo_client):
    return mongo_client.get_database("admin").command({"ping": 1})



