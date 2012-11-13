__author__ = 'abdul'

# Contains mongo db utility functions
import pymongo
from mongo_uri_tools import parse_mongo_uri
from bson.son import SON

from date_utils import timedelta_total_seconds
import mbs_logging

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
def mongo_connect(uri):
    try:
        uri_wrapper = parse_mongo_uri(uri)
        dbname = uri_wrapper.database
        if not dbname:
            dbname = "admin"
            if uri.endswith("/"):
                uri += "admin"
            else:
                uri += "/admin"

        return pymongo.Connection(uri)[dbname]
    except Exception, e:
        raise Exception("Could not establish a database connection to "
                        "%s: %s" % (uri_wrapper.masked_uri, e))

###############################################################################
def get_best_source_member(cluster_uri):
    """
        Returns the best source member to get the pull from.
        This only applicable for cluster connections.
        best = passives with least lags, if no passives then least lag

    """
    members = get_cluster_members(cluster_uri)
    secondaries = []
    primary = None

    # find primary/secondaries
    for member in members:
        if member.is_primary():
            primary = member
        elif member.is_secondary():
            secondaries.append(member)

    if not primary:
        raise Exception("Unable to determine primary for cluster '%s'" %
                        cluster_uri)

    if not secondaries:
        raise Exception("No secondaries found for cluster '%s'" %
                        cluster_uri)

    master_status = primary.rs_status
    # compute lags
    for secondary in secondaries:
        secondary.compute_lag(master_status)

    def best_secondary_comp(member1, member2):

        if member1.is_passive():
            if member2.is_passive():
                return member1.lag - member2.lag
            else:
                return -1
        elif member2.is_passive():
            return 1
        else:
            return member1.lag - member2.lag


    secondaries.sort(best_secondary_comp)
    best_member = secondaries[0]
    return best_member


###############################################################################
def get_cluster_members(cluster_uri):
    uri_wrapper = parse_mongo_uri(cluster_uri)
    # validate that uri has DB set to admin or nothing
    if uri_wrapper.database and uri_wrapper.database != "admin":
        raise Exception("Database in uri '%s' can only be admin or"
                        " unspecified" % uri_wrapper.masked_uri)
    members = []
    for member_uri in uri_wrapper.member_raw_uri_list:
        members.append(MongoServer(member_uri))

    return members

###############################################################################
class MongoServer(object):
###############################################################################

    ###########################################################################
    def __init__(self, uri):
        self._uri_wrapper = parse_mongo_uri(uri)
        self._admin_db = mongo_connect(uri)
        self._rs_conf = self._get_rs_config()
        self._rs_status = self._get_rs_status()
        self._member_config = self._get_member_config()
        self._lag = 0

    ###########################################################################
    @property
    def uri(self):
        return self._uri_wrapper.raw_uri

    ###########################################################################
    @property
    def address(self):
        return self._uri_wrapper.addresses[0]

    ###########################################################################
    @property
    def lag(self):
        return self._lag

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
            raise Exception("Unable to determine replicaset status for"
                                " member '%s'" % self)

        lag_in_seconds = abs(timedelta_total_seconds(
            master_status['optimeDate'] -
            my_status['optimeDate']))

        self._lag = lag_in_seconds
        return self._lag

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
            Returns true if the member is secondary
        """
        master_result = self._admin_db.command({"isMaster" : 1})
        return master_result and master_result.get("secondary")

    ###########################################################################
    @property
    def database_total_stats(self):
        """
            Returns true if the member is secondary
        """
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
        database_names = self._admin_db.connection.database_names()
        for dbname in database_names:
            db_stats = self._get_database_stats(dbname)
            for key in total_stats.keys():
                    total_stats[key] += db_stats.get(key) or 0

        # convert size to GB
        def to_gb(bytes):
            gbs = bytes/(1024 * 1024 * 1024)
            return round(gbs, 2)

        total_stats_gb = {
            "collections": total_stats["collections"],
            "objects": total_stats["objects"],
            "dataSizeInGB": to_gb(total_stats["dataSize"]),
            "storageSizeInGB": to_gb(total_stats["storageSize"]),
            "indexes": total_stats["indexes"],
            "indexSizeInGB": to_gb(total_stats["indexSize"]),
            "fileSizeInGB": to_gb(total_stats["fileSize"]),
            "nsSizeMB": total_stats["nsSizeMB"]
        }

        return total_stats_gb

    ###########################################################################
    def _get_database_stats(self, dbname):
        return self._admin_db.connection[dbname].command({"dbstats":1})

    ###########################################################################
    def _get_rs_status(self):
        try:
            rs_status_cmd = SON([('replSetGetStatus', 1)])
            rs_status =  self._admin_db.command(rs_status_cmd)
            for member in rs_status['members']:
                if 'self' in member and member['self']:
                    return member
        except Exception, e:
            logger.error("Cannot get rs for member '%s'. cause: %s" %
                        (self, e))
            return None

    ###########################################################################
    def _get_rs_config(self):

        try:
            local_db = self._admin_db.connection["local"]
            return local_db['system.replset'].find_one()
        except Exception, e:
                logger.error("Cannot get rs config for member '%s'. "
                            "cause: %s" % (self, e))


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