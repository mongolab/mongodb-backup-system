__author__ = 'abdul'


import os
import time

import logging
import operator
import re

from mbs import get_mbs


from persistence import update_backup, update_restore
from mongo_utils import (
    MongoCluster, MongoServer, ShardedClusterConnector,
    MongoNormalizedVersion, build_mongo_connector)

from date_utils import timedelta_total_seconds, date_now, mid_date_between, date_plus_seconds

from subprocess import CalledProcessError
from errors import *
from utils import (which, ensure_dir, execute_command, execute_command_wrapper, safe_stringify,
                   listify, list_dir_subdirs, document_pretty_string)

from source import CompositeBlockStorage, is_snapshot_volume_encrypted

from target import (
    SnapshotStatus, multi_target_upload_file,
    EbsSnapshotReference, CompositeBlockStorageSnapshotReference
)


from globals import EventType
from robustify.robustify import robustify
from naming_scheme import *
from threading import Thread

from bson.son import SON

import backup_assistant
from backup import Backup

###############################################################################
# CONSTANTS
###############################################################################

# max number of retries
MAX_NO_RETRIES = 3

MAX_LOCK_TIME = 60 # seconds

EVENT_START_EXTRACT = "START_EXTRACT"
EVENT_END_EXTRACT = "END_EXTRACT"
EVENT_START_ARCHIVE = "START_ARCHIVE"
EVENT_END_ARCHIVE = "END_ARCHIVE"
EVENT_START_UPLOAD = "START_UPLOAD"
EVENT_END_UPLOAD = "END_UPLOAD"

# max time to wait for balancer to stop (10 minutes)
MAX_BALANCER_STOP_WAIT = 30 * 60

# default max lag
DEFAULT_MAX_LAG = 5 * 60

###############################################################################
VERSION_2_6 = MongoNormalizedVersion("2.6.0")
VERSION_3_0 = MongoNormalizedVersion("3.0.0")

###############################################################################
# Member preference values


class MemberPreference(object):
    PRIMARY_ONLY = "PRIMARY_ONLY"
    SECONDARY_ONLY = "SECONDARY_ONLY"
    BEST = "BEST"
    NOT_PRIMARY = "NOT_PRIMARY"


###############################################################################
# Backup mode values


class BackupMode(object):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
###############################################################################
class BackupEventNames(object):
    FSYNCLOCK = "FSYNCLOCK"
    FSYNCLOCK_END = "FSYNCLOCK_END"
    FSYNCUNLOCK = "FSYNCUNLOCK"
    FSYNCUNLOCK_END = "FSYNCUNLOCK_END"
    SUSPEND_IO = "SUSPEND_IO"
    SUSPEND_IO_END = "SUSPEND_IO_END"
    RESUME_IO = "RESUME_IO"
    RESUME_IO_END = "RESUME_IO_END"

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())



###############################################################################
# BackupStrategy Classes
###############################################################################
class BackupStrategy(MBSObject):

    ###########################################################################
    def __init__(self):
        MBSObject.__init__(self)
        self._member_preference = None
        self._ensure_localhost = None
        self._max_data_size = None
        self._backup_name_scheme = None
        self._backup_description_scheme = None

        self._use_fsynclock = None
        self._use_suspend_io = None

        self._allow_offline_backups = None
        self._backup_mode = None

        self._max_lock_time = MAX_LOCK_TIME

        self._max_lag_seconds = None

        self._backup_assistant = None
        self._disable_source_stats = None

        self._no_index_restore = None
        self._no_users_restore = None
        self._no_roles_restore = None

    ###########################################################################
    def _init_strategy(self, backup):

        logger.info("Init Strategy settings for backup %s ..." % backup.id)

        self.member_preference = (self.member_preference or
                                  MemberPreference.BEST)

        self.backup_mode = self.backup_mode or BackupMode.ONLINE

    ###########################################################################
    @property
    def member_preference(self):
        return self._member_preference

    @member_preference.setter
    def member_preference(self, val):
        self._member_preference = val

    ###########################################################################
    @property
    def ensure_localhost(self):
        return self._ensure_localhost

    @ensure_localhost.setter
    def ensure_localhost(self, val):
        self._ensure_localhost = val

    ###########################################################################
    @property
    def max_data_size(self):
        return self._max_data_size

    @max_data_size.setter
    def max_data_size(self, val):
        self._max_data_size = val

    ###########################################################################
    @property
    def max_lag_seconds(self):
        return self._max_lag_seconds

    @max_lag_seconds.setter
    def max_lag_seconds(self, val):
        self._max_lag_seconds = val

    ###########################################################################
    @property
    def disable_source_stats(self):
        return self._disable_source_stats

    @disable_source_stats.setter
    def disable_source_stats(self, val):
        self._disable_source_stats = val

    ###########################################################################
    @property
    def backup_name_scheme(self):
        return self._backup_name_scheme

    @backup_name_scheme.setter
    def backup_name_scheme(self, naming_scheme):
        if isinstance(naming_scheme, (unicode, str)):
            naming_scheme = TemplateBackupNamingScheme(template=naming_scheme)

        self._backup_name_scheme = naming_scheme

    ###########################################################################
    @property
    def backup_description_scheme(self):
        return self._backup_description_scheme

    @backup_description_scheme.setter
    def backup_description_scheme(self, naming_scheme):
        if isinstance(naming_scheme, (unicode, str)):
            naming_scheme = TemplateBackupNamingScheme(template=naming_scheme)

        self._backup_description_scheme = naming_scheme

    ###########################################################################
    @property
    def use_fsynclock(self):
        return self._use_fsynclock

    @use_fsynclock.setter
    def use_fsynclock(self, val):
        self._use_fsynclock = val

    ###########################################################################
    @property
    def use_suspend_io(self):
        return self._use_suspend_io

    @use_suspend_io.setter
    def use_suspend_io(self, val):
        self._use_suspend_io = val

    ###########################################################################
    @property
    def allow_offline_backups(self):
        return self._allow_offline_backups

    @allow_offline_backups.setter
    def allow_offline_backups(self, val):
        self._allow_offline_backups = val


    ###########################################################################
    @property
    def backup_mode(self):
        return self._backup_mode

    @backup_mode.setter
    def backup_mode(self, val):
        self._backup_mode = val

    ###########################################################################
    @property
    def backup_assistant(self):
        if not self._backup_assistant:
            self._backup_assistant = get_mbs().default_backup_assistant
        return self._backup_assistant

    @backup_assistant.setter
    def backup_assistant(self, val):
        self._backup_assistant = val

    ###########################################################################
    @property
    def no_index_restore(self):
        return self._no_index_restore

    @no_index_restore.setter
    def no_index_restore(self, val):
        self._no_index_restore = val

    ###########################################################################
    @property
    def no_users_restore(self):
        return self._no_users_restore

    @no_users_restore.setter
    def no_users_restore(self, val):
        self._no_users_restore = val

    ###########################################################################
    @property
    def no_roles_restore(self):
        return self._no_roles_restore

    @no_roles_restore.setter
    def no_roles_restore(self, val):
        self._no_roles_restore = val

    ###########################################################################
    def is_use_suspend_io(self):
        return False

    ###########################################################################
    def run_backup(self, backup):
        self._init_strategy(backup)
        self._do_run_backup(backup)

    ###########################################################################
    def _do_run_backup(self, backup):
        mongo_connector = self.get_backup_mongo_connector(backup)
        self.backup_mongo_connector(backup, mongo_connector)

    ###########################################################################
    def get_backup_mongo_connector(self, backup):
        source_connector = backup.source.get_connector()
        selected_connector = self.select_backup_mongo_connector(backup, source_connector)
        self._populate_connector_warnings(backup, selected_connector)

        # set the selected source
        selected_sources = backup.source.get_selected_sources(selected_connector)
        self._validate_selected_sources(selected_sources, backup)

        backup.selected_sources = selected_sources
        update_backup(backup, properties="selectedSources", event_name="SELECT_SOURCES",
                      message="Selected backup sources")

        if isinstance(source_connector, MongoCluster):
            # grab cluster stats
            self._compute_cluster_stats(backup, source_connector)

        return selected_connector

    ###########################################################################
    def _validate_selected_sources(self, selected_sources, backup):
        # ensure that the selected sources are unique
        selected_ids = []
        for source in selected_sources:
            if source.id in selected_ids:
                raise Exception("source %s has been selected twice!!!" % source.id)
            selected_ids.append(source.id)

    ###########################################################################
    def select_backup_mongo_connector(self, backup, source_connector):
        logger.info("Selecting connector to run backup '%s'" % backup.id)
        if isinstance(source_connector, MongoCluster):
            selected_connector = self._select_backup_cluster_member(backup, source_connector)
        elif isinstance(source_connector, ShardedClusterConnector):
            selected_connector = self._select_backup_sharded_cluster_members(backup, source_connector)
        else:
            selected_connector = source_connector

        self._validate_connector(backup, source_connector, selected_connector)

        logger.info("Selected connector %s for backup '%s'" %
                    (selected_connector.info(), backup.id))
        return selected_connector

    ####################################################################################################################
    def _populate_connector_warnings(self, backup, connector):
        if isinstance(connector, MongoServer):
            # warn if its a primary
            if connector.is_primary():
                logger.warning("Backup '%s' will be extracted from the "
                               "primary!" % backup.id)

                msg = "Warning! The backup will be taken off of the primary"
                update_backup(backup, event_type=EventType.WARNING,
                              event_name="USING_PRIMARY_WARNING",
                              message=msg)
            # log warning if secondary is too stale
            elif connector.is_secondary():
                if connector.is_too_stale():
                    logger.warning("Backup '%s' will be taken off of a "
                                   "too stale member!" % backup.id)

                    msg = "Warning! The backup will be taken off of a too stale member"
                    update_backup(backup, event_type=EventType.WARNING,
                                  event_name="USING_TOO_STALE_WARNING",
                                  message=msg)

    ###########################################################################
    def _validate_connector(self, backup, source_connector, connector):

        if isinstance(connector, ShardedClusterConnector):
            self._validate_sharded_connector(backup, connector)
            return

        logger.info("Validate selected connector '%s'..." % connector)

        logger.info("1- validating connectivity to '%s'..." % connector)
        if not connector.is_online():
            logger.info("'%s' is offline" % connector)
            if self.allow_offline_backups:
                logger.info("allowOfflineBackups is set to true so its all "
                            "good")
                self._set_backup_mode(backup, BackupMode.OFFLINE)
                return
            elif self.backup_mode == BackupMode.ONLINE:
                msg = "Selected connector '%s' is offline" % connector
                raise InvalidSelectedMemberError(backup.source.uri, msg=msg)
        else:
            logger.info("Connector '%s' is online! Yay!" % connector)

        logger.info("Validating selected connector '%s' against member "
                    "preference '%s' for backup '%s'" %
                    (connector, self.member_preference, backup.id))

        if self.member_preference == MemberPreference.SECONDARY_ONLY:
            if not connector.is_secondary():
                msg = "Selected connector '%s' is not a secondary" % connector
                raise InvalidSelectedMemberError(backup.source.uri, msg=msg)

        if (self.member_preference == MemberPreference.PRIMARY_ONLY and
                not connector.is_primary()):
            msg = "Selected connector '%s' is not a primary" % connector
            raise InvalidSelectedMemberError(backup.source.uri, msg=msg)

        if (self.member_preference == MemberPreference.NOT_PRIMARY and
                connector.is_primary()):
            msg = "Selected connector '%s' is a Primary" % connector
            raise InvalidSelectedMemberError(backup.source.uri, msg=msg)

        # FAIL if best secondary was not a P0 within max_lag_seconds
        # if cluster has any P0 (excluding slave delay)
        max_lag_seconds = self._max_allowed_lag_for_backup(backup)

        if isinstance(source_connector, MongoCluster) and max_lag_seconds and connector.priority != 0:
            rs_conf = source_connector.primary_member.rs_conf
            for member in source_connector.members:
                if (member.is_online() and
                        not source_connector.is_member_not_eligible_for_backups(member, rs_conf) and
                        (member.priority == 0 or member.hidden) and not member.slave_delay):
                    msg = ("No eligible p0 secondary found within max lag '%s'"
                           " for cluster '%s'" % (max_lag_seconds, source_connector))
                    self.raise_no_eligible_members_found(source_connector, msg=msg)

        logger.info("Member preference validation for backup '%s' passed!" %
                    backup.id)

    ###########################################################################
    def _validate_sharded_connector(self, backup, sharded_connector):
        for shard, selected_connector in zip(sharded_connector.shards,
                                             sharded_connector.selected_shard_secondaries):
            self._validate_connector(backup, shard, selected_connector)

    ###########################################################################
    def _select_backup_cluster_member(self, backup, mongo_cluster):
        logger.info("Selecting a member from cluster '%s' for backup '%s' "
                    "using pref '%s'" %
                    (backup.id, mongo_cluster, self.member_preference))
        if not self._needs_new_member_selection(backup):
            logger.info("Using previous selected member for backup '%s' " %
                        backup.id)
            return self.get_mongo_connector_used_by(backup)
        else:
            return self._select_new_cluster_member(backup, mongo_cluster)

    ###########################################################################
    def _select_backup_sharded_cluster_members(self, backup, sharded_cluster):
        self.select_shard_config_server(sharded_cluster)
        # MAX LAG HAS TO BE 5 for sharded backups!
        # select best secondaries within shards
        self.select_shard_best_secondaries(sharded_cluster, max_lag_seconds=5)

        return sharded_cluster

    ###########################################################################
    def select_shard_best_secondaries(self, sharded_cluster, max_lag_seconds=None):
        best_secondaries = []

        for shard in sharded_cluster.shards:
            shard_best = self.get_cluster_best_secondary(shard, max_lag_seconds=max_lag_seconds)
            if not shard_best:
                raise NoEligibleMembersFound(shard.uri, msg="No best secondary found within max lag "
                                                            "for shard '%s'" % shard.connector_id)
            best_secondaries.append(shard_best)

        sharded_cluster.selected_shard_secondaries = best_secondaries

        return best_secondaries

    ###########################################################################
    def select_shard_config_server(self, sharded_cluster):
        if isinstance(sharded_cluster.config_servers, MongoCluster):
            sharded_cluster.config_server = self.get_cluster_best_secondary(sharded_cluster.config_servers, max_lag_seconds=0)
        elif isinstance(sharded_cluster.config_servers, list):
            for conf_server in sharded_cluster.config_servers:
                if isinstance(conf_server, MongoServer) and conf_server.is_online():
                    sharded_cluster.config_server = conf_server
                    break

        if sharded_cluster.config_server is None:
            raise Exception("No online config servers found for '%s'" % sharded_cluster)

    ###########################################################################
    def _needs_new_member_selection(self, backup):
        """
            Needs to be implemented by subclasses
        """
        return True

    ####################################################################################################################
    def _max_allowed_lag_for_backup(self, backup):
        max_lag_seconds = self.max_lag_seconds
        # compute max lag
        if not max_lag_seconds:
            if backup.plan:
                max_lag_seconds = backup.plan.schedule.max_acceptable_lag(backup.plan_occurrence)
            else:
                max_lag_seconds = DEFAULT_MAX_LAG

        return max_lag_seconds

    ####################################################################################################################
    def get_mongo_connector_used_by(self, backup):
        if backup.selected_sources and len(backup.selected_sources) == 1:
            return backup.selected_sources[0].get_connector()

        uri_wrapper = mongo_uri_tools.parse_mongo_uri(backup.source.uri)
        if backup.source_stats:
            if backup.source_stats.get("repl"):
                host = backup.source_stats["repl"]["me"]
            else:
                host = backup.source_stats["host"]
            if uri_wrapper.username:
                credz = "%s:%s@" % (uri_wrapper.username, uri_wrapper.password)
            else:
                credz = ""


            if uri_wrapper.database:
                db_str = "/%s" % uri_wrapper.database
            else:
                db_str = ""
            uri = "mongodb://%s%s%s" % (credz, host, db_str)

            return build_mongo_connector(uri)

    ####################################################################################################################
    def _select_new_cluster_member(self, backup, mongo_cluster):
        max_lag_seconds = self._max_allowed_lag_for_backup(backup)

        # get primary member
        primary_member = mongo_cluster.primary_member

        # SECONDARY_ONLY
        if self.member_preference == MemberPreference.SECONDARY_ONLY:
            return self.get_cluster_best_secondary(mongo_cluster, max_lag_seconds=max_lag_seconds)
        # BEST
        elif self.member_preference == MemberPreference.BEST:
            try:
                return self.get_cluster_best_secondary(mongo_cluster, max_lag_seconds=max_lag_seconds)
            except NoEligibleMembersFound, ne:
                logger.error(safe_stringify(ne))
                return primary_member
        # PRIMARY ONLY
        elif self.member_preference == MemberPreference.PRIMARY_ONLY:
            return primary_member
        else:
            self.raise_no_eligible_members_found(mongo_cluster, "Can't find any members for pref %s" %
                                                 self.member_preference)

    ####################################################################################################################
    def raise_no_eligible_members_found(self, mongo_cluster, msg, error_type=None):
        rs_conf = None
        rs_status = None
        error_type = error_type or NoEligibleMembersFound
        try:
            rs_status = mongo_cluster.primary_member.get_rs_status()
        finally:
            try:
                rs_conf = mongo_cluster.primary_member.rs_conf
            finally:
                pass
        raise error_type(mongo_cluster.uri, msg, rs_status=rs_status, rs_conf=rs_conf)


    ####################################################################################################################
    def get_cluster_best_secondary(self, mongo_cluster, max_lag_seconds=None):
        """
            Returns the best source member to get the pull from.
            This only applicable for cluster connections.
            best = passives with least lags, if no passives then least lag
        """
        members = mongo_cluster.members
        rs_conf = mongo_cluster.primary_member.rs_conf

        all_secondaries = []
        hidden_secondaries = []
        p0_secondaries = []
        other_secondaries = []

        # check if there is a mongolab node
        backup_node = mongo_cluster.get_mongolab_backup_node()

        if backup_node:
            logger.info("Found mongolabBackupNode '%s'. Validating ..." %
                        backup_node)
            # Ah! validate it if meets the conditions
            self._validate_cluster_backup_node(mongo_cluster, backup_node, max_lag_seconds)
            logger.info("mongolabBackupNode '%s' is valid! Returning as the "
                        "best secondary for '%s'" % (backup_node, self))
            return backup_node

        # find secondaries
        for member in members:
            try:
                if not member.is_online():
                    logger.info("Member '%s' appears to be offline. "
                                "Excluding..." % member)
                    continue
                elif member.is_secondary():
                    if member.slave_delay:
                        logger.info("Member '%s' appears to have slave delay. "
                                    "Excluding..." % member)
                        continue
                    elif mongo_cluster.is_member_not_eligible_for_backups(member, rs_conf):
                        logger.info("Member '%s' is tagged to be not eligible for backups. "
                                    "Excluding..." % member)
                        continue
                    all_secondaries.append(member)

                    if member.hidden:
                        hidden_secondaries.append(member)
                    elif member.priority == 0:
                        p0_secondaries.append(member)
                    else:
                        other_secondaries.append(member)
            except Exception, ex:
                logger.exception("get_cluster_best_secondary(): Cannot determine "
                                 "lag for '%s'. Skipping " % member)

        if not all_secondaries:
            self.raise_no_eligible_members_found(mongo_cluster, "No secondaries found for cluster '%s'" % mongo_cluster)

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
        master_status = mongo_cluster.primary_member.member_rs_status
        if merged_list:
            for secondary in merged_list:
                # compute lags
                # TODO compute lag should only rs conf/status from primary only! this should also be applied to all
                # other rs conf/status operations
                secondary.compute_lag(master_status)
                if max_lag_seconds is None:
                    return secondary
                elif secondary.lag_in_seconds <= max_lag_seconds:
                    return secondary
                else:
                    logger.info("Excluding secondary '%s' because its lagging %s seconds which is more than maximum"
                                " allowed lag '%s'" % (secondary, secondary.lag_in_seconds, max_lag_seconds))

        self.raise_no_eligible_members_found(mongo_cluster,
                                             "No secondaries found for cluster %s within max allowed lag %s" %
                                             (mongo_cluster, max_lag_seconds),
                                             error_type=NoSecondariesWithinMaxLagError)

    ####################################################################################################################
    def _validate_cluster_backup_node(self, mongo_cluster, backup_node, max_lag_seconds=None):
        master_status = mongo_cluster.primary_member.member_rs_status
        if not backup_node.is_online():
            self.raise_no_eligible_members_found(mongo_cluster, "Backup Node '%s' is offline" % backup_node)

        if not backup_node.is_secondary():
            self.raise_no_eligible_members_found(mongo_cluster, "Backup Node '%s' not is not secondary" % backup_node)

        if max_lag_seconds is not None:
            backup_node.compute_lag(master_status)
            if backup_node.lag_in_seconds > max_lag_seconds:
                msg = ("Backup Node '%s' is lagging %s which is more"
                       " than max lag allowed %s" %
                       (backup_node, backup_node.lag_in_seconds,
                        max_lag_seconds))
                self.raise_no_eligible_members_found(mongo_cluster, msg, error_type=NoSecondariesWithinMaxLagError)

    ####################################################################################################################
    def _compute_cluster_stats(self, backup, mongo_cluster):
        cluster_stats = {
            "rsStatus": mongo_cluster.primary_member.get_rs_status()
        }
        backup.cluster_stats = cluster_stats

        update_backup(backup, properties="clusterStats",
                      event_name="COMPUTE_CLUSTER_STATS", message="Compute cluster stats")

    ####################################################################################################################
    def _create_backup_workspace(self, backup):
        self.backup_assistant.create_task_workspace(backup)
        if not backup.is_event_logged("CREATE_WORKSPACE"):
            update_backup(backup, event_name="CREATE_WORKSPACE", message="Creating backup workspace")

    ####################################################################################################################
    def backup_mongo_connector(self, backup, mongo_connector):

        # ensure local host if specified
        if (self.ensure_localhost and
                not self.backup_assistant.is_connector_local_to_assistant(mongo_connector, backup)):
            details = ("Source host for dump source '%s' is not running "
                       "locally and strategy.ensureLocalHost is set to true" %
                       mongo_connector)
            raise BackupNotOnLocalhost(msg="Error while attempting to dump",
                                       details=details)

        # record stats
        if (not self.disable_source_stats and
            (not backup.source_stats or self._needs_new_source_stats(backup))):
            self._compute_source_stats(backup, mongo_connector)

        # set backup name and description
        self._set_backup_name_and_desc(backup)

        # validate max data size if set
        self._validate_max_data_size(backup)

        # backup the mongo connector
        self.do_backup_mongo_connector(backup, mongo_connector)

        # calculate backup rate
        self._calculate_backup_rate(backup)

    ###########################################################################
    def _compute_source_stats(self, backup, mongo_connector):
        """
        computes backup source stats
        :param backup:
        :param mongo_connector:
        :return:
        """
        logger.info("Computing source stats for backup '%s' , connector '%s'..." % (backup.id, mongo_connector))
        dbname = backup.source.database_name
        try:
            if (self.backup_mode == BackupMode.ONLINE and
                    mongo_connector.is_online()):
                backup.source_stats = mongo_connector.get_stats(
                    only_for_db=dbname)
                # save source stats
                update_backup(backup, properties="sourceStats",
                              event_name="COMPUTED_SOURCE_STATS",
                              message="Computed source stats")

                logger.info("Finished computing source stats for backup '%s' , connector '%s'." % (backup.id,
                                                                                                   mongo_connector))
        except Exception, e:
            if is_connection_exception(e) and self.allow_offline_backups:
                # switch to offline mode
                logger.info("Caught a connection error while trying to compute"
                            " source stats for backup '%s'. %s. Switching to "
                            "OFFLINE mode..." % (backup.id, e))
                self._set_backup_mode(backup, BackupMode.OFFLINE)
            else:
                raise

    ###########################################################################
    def _set_backup_mode(self, backup, mode):
        """
            sets/persists specified backup mode
        """
        logger.info("Update backup '%s'. Set backup mode to '%s'." %
                    (backup.id, mode))
        self.backup_mode = mode
        # save source stats
        update_backup(backup, properties="strategy",
                      event_name="SET_BACKUP_MODE",
                      message="Setting backup mode to '%s'" % mode)

    ###########################################################################
    def _needs_new_source_stats(self, backup):
        """
            Needs to be implemented by subclasses
        """
        return True

    ###########################################################################
    def do_backup_mongo_connector(self, backup, mongo_connector):
        """
            Does the actual work. Has to be overridden by subclasses
        """

    ###########################################################################
    def _calculate_backup_rate(self, backup):
        logger.info("Calculating backup rate for backup '%s'..." % backup.id)
        duration = timedelta_total_seconds(date_now() - backup.start_date)
        if backup.source_stats and backup.source_stats.get("dataSize"):
            size_mb = float(backup.source_stats["dataSize"]) / (1024 * 1024)
            rate = size_mb/duration
            rate = round(rate, 2)
            if rate:
                backup.backup_rate_in_mbps = rate
                # save changes
                update_backup(backup, properties="backupRateInMbps")


    ###########################################################################
    def cleanup_backup(self, backup):

        # delete the temp dir
        logger.info("Running Cleanup for backup %s" % backup.id)
        update_backup(backup, event_name="CLEANUP", message="Running cleanup")

        if backup.is_event_logged("CREATE_WORKSPACE"):
            self.backup_assistant.delete_task_workspace(backup)

    ###########################################################################
    def run_restore(self, restore):
        self._do_run_restore(restore)
        self._compute_restore_destination_stats(restore)

    ###########################################################################
    def _do_run_restore(self, restore):
        """
            Does the actual restore. Must be overridden by subclasses
        """

    ###########################################################################
    def cleanup_restore(self, restore):

        logger.info("Running Cleanup for restore %s" % restore.id)
        update_restore(restore, event_name="CLEANUP",
                       message="Running cleanup")

        self.backup_assistant.delete_task_workspace(restore)

    ###########################################################################
    def _compute_restore_destination_stats(self, restore):
        logger.info("Computing destination stats for restore '%s'" %
                    restore.id)
        dest_connector = restore.destination.get_connector()
        dbname = restore.destination.database_name
        restore.destination_stats = dest_connector.get_stats(only_for_db=dbname)
        update_restore(restore, properties=["destinationStats"])

    ###########################################################################
    # Helpers
    ###########################################################################
    def _validate_max_data_size(self, backup):
        logger.info("Validating backup '%s' against max data size..." % backup.id)
        if (self.max_data_size and
            backup.source_stats and
            backup.source_stats.get("dataSize") and
            backup.source_stats.get("dataSize") > self.max_data_size):

            data_size = backup.source_stats.get("dataSize")
            database_name = backup.source.database_name
            raise SourceDataSizeExceedsLimits(data_size=data_size,
                                              max_size=self.max_data_size,
                                              database_name=database_name)
        logger.info("max data size validation for backup '%s' passed!" % backup.id)


    ###########################################################################
    def _fsynclock(self, backup, mongo_connector):
        if isinstance(mongo_connector, MongoServer):
            msg = ("Running fsynclock on '%s' (connection '%s')" %
                   (mongo_connector,
                    mongo_connector.connection_id))
            logger.info(msg)
            update_backup(backup, event_name=BackupEventNames.FSYNCLOCK, message=msg)
            mongo_connector.fsynclock()
            update_backup(backup, event_name=BackupEventNames.FSYNCLOCK_END, message="fsynclock done!")
            self._start_max_fsynclock_monitor(backup, mongo_connector)
        else:
            raise ConfigurationError("Invalid fsynclock attempt. '%s' has to"
                                     " be a MongoServer" % mongo_connector)

    ###########################################################################
    # NOTE: Unlock is very important so # of retries is set to a high number to ensure (12
    # we try
    def _fsyncunlock(self, backup, mongo_connector):
        if isinstance(mongo_connector, MongoServer):
            msg = ("Running fsyncunlock on '%s' (connection '%s')" %
                   (mongo_connector,
                    mongo_connector.connection_id))
            logger.info(msg)
            update_backup(backup, event_name=BackupEventNames.FSYNCUNLOCK, message=msg)
            mongo_connector.fsyncunlock()
            update_backup(backup, event_name=BackupEventNames.FSYNCUNLOCK_END, message="fsyncunlock done!")
        else:
            raise ConfigurationError("Invalid fsyncunlock attempt. '%s' has to"
                                     " be a MongoServer" % mongo_connector)

    ###########################################################################
    def _start_max_fsynclock_monitor(self, backup, mongo_connector):
        def max_lock_monitor(strategy, bkp, connector):
            time.sleep(self._max_lock_time)
            logger.info("MaxFsynclockMonitor: Max time is up, checking if"
                        " server '%s' is locked..." % connector)
            if connector.is_server_locked():
                try:
                    msg = ("MaxFsynclockMonitor: %s has been locked for more"
                           " than max allowed time (%s seconds)!!"
                           " Unlocking ..." % (connector, self._max_lock_time))
                    logger.error(msg)
                    update_backup(bkp, event_name="FSYNC_LOCK_MONITOR",
                                  message=msg,
                                  event_type=EventType.ERROR)
                    strategy._fsyncunlock(backup, connector)
                except Exception, e:
                    logger.exception("MaxFsynclockMonitor")
            else:
                logger.info("MaxFsynclockMonitor: All good. Server '%s' is was"
                            " unlocked within max threshold..." %
                            mongo_connector)

        logger.info("Starting MaxFsynclockMonitor...")
        Thread(target=max_lock_monitor,
               args=[self, backup, mongo_connector]).start()

    ###########################################################################
    def _suspend_io(self, backup, mongo_connector, cloud_block_storage,
                    ensure_local=True):

        if not isinstance(mongo_connector, MongoServer):
            raise ConfigurationError("Invalid suspend io attempt. '%s' has to"
                                     " be a MongoServer" % mongo_connector)

        if ensure_local and not self.backup_assistant.is_connector_local_to_assistant(mongo_connector, backup):
            err = ("Cannot suspend io for '%s' because is not local to"
                   " this box" % mongo_connector)
            raise ConfigurationError(err)

        try:

            msg = "Running suspend IO for '%s'..." % mongo_connector
            logger.info(msg)
            update_backup(backup, event_name=BackupEventNames.SUSPEND_IO, message=msg)
            self.backup_assistant.suspend_io(backup, mongo_connector, cloud_block_storage)

            update_backup(backup, event_name=BackupEventNames.SUSPEND_IO_END, message="Suspend IO done!")


        except Exception, ex:
            msg = ("Suspend IO Error for '%s'" % mongo_connector)
            logger.exception(msg)
            if isinstance(ex, MBSError):
                raise
            else:
                raise SuspendIOError(msg, cause=ex)
        finally:
            self._start_max_io_suspend_monitor(backup, mongo_connector,
                                               cloud_block_storage)

    ###########################################################################
    def _start_max_io_suspend_monitor(self, backup, mongo_connector,
                                      cloud_block_storage):

        def max_suspend_monitor(bkp, connector, cbs):
            time.sleep(self._max_lock_time)
            logger.info("MaxIOSuspendMonitor: Max time is up, checking if"
                        " server '%s' IO is suspended..." %
                        mongo_connector)
            # TODO: currently, there is no way of telling if io is suspended
            # so we always blindly resume. If resume succeeds then we log an
            # error :)
            try:
                self.backup_assistant.resume_io(bkp, connector, cbs)
                msg = ("MaxIOSuspendMonitor: %s IO has been suspended for "
                       "more than max allowed time (%s seconds)!!"
                       " Resuming ..." % (connector,
                                          self._max_lock_time))
                logger.error(msg)
                update_backup(bkp,
                              event_name="IO_SUSPEND_MONITOR_MONITOR",
                              message=msg,
                              event_type=EventType.ERROR)

            except Exception, e:
                logger.info("MaxIOSuspendMonitor: It appears that server "
                            "'%s' IO was resumed within max threshold." %
                            connector)

        logger.info("Starting MaxIOSuspendMonitor...")
        Thread(target=max_suspend_monitor,
               args=[backup, mongo_connector, cloud_block_storage]).start()

    ###########################################################################
    def _resume_io(self, backup, mongo_connector, cloud_block_storage,
                   ensure_local=True):

        if not isinstance(mongo_connector, MongoServer):
            raise ConfigurationError(
                "Invalid resume io attempt. '%s' has to be a MongoServer" %
                mongo_connector)

        if ensure_local and not self.backup_assistant.is_connector_local_to_assistant(mongo_connector, backup):
            err = ("Cannot resume io for '%s' because is not local to "
                   "this box" % mongo_connector)
            raise ConfigurationError(err)

        try:
            msg = "Running resume io for '%s'" % mongo_connector
            update_backup(backup, event_name=BackupEventNames.RESUME_IO, message=msg)
            self.backup_assistant.resume_io(backup, mongo_connector, cloud_block_storage)
        except Exception, ex:
            msg = ("Resume IO Error for '%s'" % mongo_connector)
            logger.exception(msg)
            if isinstance(ex, MBSError):
                raise
            else:
                raise ResumeIOError(msg, cause=ex)



    ###########################################################################
    def _stop_balancer(self, backup, sharded_connector):

        if sharded_connector.is_balancer_active():
            msg = "Stopping balancer for '%s'" % backup.source.id
            logger.info(msg)
            update_backup(backup, event_name="STOP_BALANCER", message=msg)
            sharded_connector.stop_balancer()

            count = 0
            while (sharded_connector.is_balancer_active() and
                           count < MAX_BALANCER_STOP_WAIT):
                logger.info("Waiting for balancer to stop..")
                time.sleep(5)
                count += 1

            if sharded_connector.is_balancer_active():
                raise BalancerActiveError("Balancer did not stop in %s "
                                          "seconds" % MAX_BALANCER_STOP_WAIT)
            else:
                logger.info("Balancer stopped!")
        else:
            msg = ("Balancer already stopped for '%s'" %
                   backup.source.id)
            logger.info(msg)
            update_backup(backup, event_name="BALANCER_ALREADY_STOPPED",
                          message=msg)

    ###########################################################################
    def _resume_balancer(self, backup, sharded_connector):

        msg = "Resuming balancer for '%s'" % backup.source.id
        update_backup(backup, event_name="RESUME_BALANCER", message=msg)
        sharded_connector.resume_balancer()

        count = 0
        while not sharded_connector.is_balancer_active() and count < 30:
            logger.info("Waiting for balancer to resume..")
            time.sleep(1)
            count += 1

        if not sharded_connector.is_balancer_active():
            raise BalancerActiveError("Balancer did not resume in 30 seconds")
        else:
            logger.info("Balancer resumed!")


    ###########################################################################
    def _set_backup_name_and_desc(self, backup, update=False):
        logger.info("Setting backup name and description for backup '%s'" % backup.id)

        # update backup name and desc
        update_props = list()

        name = self.get_backup_name(backup)
        if not backup.name or (update and name != backup.name):
            backup.name = name
            update_props.append("name")

        desc = self.get_backup_description(backup)
        if not backup.description or (update and desc != backup.description):
            backup.description = desc
            update_props.append("description")

        if update_props:
            update_backup(backup, properties=update_props)

    ###########################################################################
    def get_backup_name(self, backup):
        return self._generate_name(backup, self.backup_name_scheme)

    ###########################################################################
    def get_backup_description(self, backup):
        return self._generate_name(backup, self.backup_description_scheme)

    ###########################################################################
    def _generate_name(self, backup, naming_scheme):
        if not naming_scheme:
            naming_scheme = DefaultBackupNamingScheme()
        elif type(naming_scheme) in [unicode, str]:
            name_template = naming_scheme
            naming_scheme = TemplateBackupNamingScheme(template=name_template)

        return naming_scheme.generate_name(backup,
                                           **backup_format_bindings(backup))

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "memberPreference": self.member_preference,
            "backupMode": self.backup_mode
        }

        if self.ensure_localhost is not None:
            doc["ensureLocalhost"] = self.ensure_localhost

        if self.max_data_size:
            doc["maxDataSize"] = self.max_data_size

        if self.max_lag_seconds:
            doc["maxLagSeconds"] = self.max_lag_seconds

        if self.disable_source_stats:
            doc["disableSourceStats"] = self.disable_source_stats

        if self.backup_name_scheme:
            doc["backupNameScheme"] = \
                self.backup_name_scheme.to_document(display_only=False)

        if self.backup_description_scheme:
            doc["backupDescriptionScheme"] =\
                self.backup_description_scheme.to_document(display_only=False)

        if self.use_fsynclock is not None:
            doc["useFsynclock"] = self.use_fsynclock

        if self.use_suspend_io is not None:
            doc["useSuspendIO"] = self.use_suspend_io

        if self.allow_offline_backups is not None:
            doc["allowOfflineBackups"] = self.allow_offline_backups

        if (self.backup_assistant is not None and
            not isinstance(self.backup_assistant, backup_assistant.LocalBackupAssistant)):
            doc["backupAssistant"] = self.backup_assistant.to_document()

        if self.no_index_restore is not None:
            doc["noIndexRestore"] = self.no_index_restore

        if self.no_users_restore is not None:
            doc["noUsersRestore"] = self.no_users_restore

        if self.no_roles_restore is not None:
            doc["noRolesRestore"] = self.no_roles_restore

        return doc

###############################################################################
# Dump Strategy Classes
###############################################################################
class DumpStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        BackupStrategy.__init__(self)
        self._force_table_scan = None
        self._dump_users = None
        self._dump_options_overrides = None
        self._restore_options_overrides = None

    ###########################################################################
    @property
    def force_table_scan(self):
        return self._force_table_scan

    @force_table_scan.setter
    def force_table_scan(self, val):
        self._force_table_scan = val

    ###########################################################################
    @property
    def dump_users(self):
        return self._dump_users

    @dump_users.setter
    def dump_users(self, val):
        self._dump_users = val


    ###########################################################################
    @property
    def dump_options_overrides(self):
        return self._dump_options_overrides

    @dump_options_overrides.setter
    def dump_options_overrides(self, val):
        self._dump_options_overrides = val

    ###########################################################################
    @property
    def restore_options_overrides(self):
        return self._restore_options_overrides

    @restore_options_overrides.setter
    def restore_options_overrides(self, val):
        self._restore_options_overrides = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = BackupStrategy.to_document(self, display_only=display_only)
        doc.update({
            "_type": "DumpStrategy"
        })

        if self.force_table_scan is not None:
            doc["forceTableScan"] = self.force_table_scan

        if self.dump_users is not None:
            doc["dumpUsers"] = self.dump_users

        return doc

    ###########################################################################
    def do_backup_mongo_connector(self, backup, mongo_connector):
        """
            Override
        """
        source = backup.source

        # create backup workspace
        self._create_backup_workspace(backup)

        # run mongoctl dump
        if not backup.is_event_logged(EVENT_END_EXTRACT):
            try:
                self.dump_backup(backup, mongo_connector,
                                 database_name=source.database_name)
                # upload dump log file
                self._upload_dump_log_file(backup)
            except DumpError, e:
                # still tar and upload failed dumps
                logger.error("Dumping backup '%s' failed. Will still tar"
                             "up and upload to keep dump logs" % backup.id)
                msg = "Dump failed. Will tar and upload the failed dump"
                update_backup(backup, event_type=EventType.ERROR, message=msg,
                              error_code=to_mbs_error_code(e))
                # TODO maybe change the name of the uploaded failed dump log
                self._upload_dump_log_file(backup)
                self._tar_and_upload_failed_dump(backup)
                raise

        # tar the dump
        if not backup.is_event_logged(EVENT_END_ARCHIVE):
            self._archive_dump(backup)

        # upload back file to the target
        if not backup.is_event_logged(EVENT_END_UPLOAD):
            self._upload_dump(backup)

    ###########################################################################
    def _archive_dump(self, backup):
        dump_dir = _backup_dump_dir_name(backup)
        tar_filename = _tar_file_name(backup)
        logger.info("Taring dump %s to %s" % (dump_dir, tar_filename))
        update_backup(backup,
                      event_name=EVENT_START_ARCHIVE,
                      message="Taring dump")

        self.backup_assistant.tar_backup(backup, dump_dir, tar_filename)

        update_backup(backup,
                      event_name=EVENT_END_ARCHIVE,
                      message="Taring completed")

    ###########################################################################
    def _upload_dump(self, backup):
        tar_file_name = _tar_file_name(backup)
        logger.info("Uploading %s to target" % tar_file_name)

        update_backup(backup,
                      event_name=EVENT_START_UPLOAD,
                      message="Upload tar to target")
        upload_dest_path = _upload_file_dest(backup)

        all_targets = [backup.target]

        if backup.secondary_targets:
            all_targets.extend(backup.secondary_targets)

        # Upload to all targets simultaneously

        target_references = self.backup_assistant.upload_backup(backup, tar_file_name, all_targets,
                                                                destination_path=upload_dest_path)

        if len(target_references) != len(all_targets):
            raise TargetUploadError("Upload target mismatch! requested to upload to %s targets and got %s target"
                                    " references back" % (len(all_targets), len(target_references)))

        # set the target reference
        target_reference = target_references[0]

        # keep old target reference if it exists to delete it because it would
        # be the failed file reference
        failed_reference = backup.target_reference
        backup.target_reference = target_reference

        # set the secondary target references
        if backup.secondary_targets:
            backup.secondary_target_references = target_references[1:]

        update_backup(backup, properties=["targetReference",
                                          "secondaryTargetReferences"],
                      event_name=EVENT_END_UPLOAD,
                      message="Upload completed!")

        # remove failed reference if exists
        if failed_reference:
            try:
                backup.target.delete_file(failed_reference)
            except Exception, ex:
                logger.error("Exception while deleting failed backup file: %s"
                             % ex)

    ###########################################################################
    def _upload_dump_log_file(self, backup):
        log_file_name = _log_file_name(backup)
        dump_dir = _backup_dump_dir_name(backup)
        logger.info("Uploading log file for %s to target" % backup.id)

        update_backup(backup, event_name="START_UPLOAD_LOG_FILE",
                      message="Upload log file to target")
        log_dest_path = _upload_log_file_dest(backup)
        log_target_reference = self.backup_assistant.upload_backup_log_file(backup, log_file_name, dump_dir,
                                                                            backup.target,
                                                                            destination_path=log_dest_path)

        backup.log_target_reference = log_target_reference

        update_backup(backup, properties="logTargetReference",
                      event_name="END_UPLOAD_LOG_FILE",
                      message="Log file upload completed!")

        logger.info("Upload log file for %s completed successfully!" %
                    backup.id)

    ###########################################################################
    def _tar_and_upload_failed_dump(self, backup):
        logger.info("Taring up failed backup '%s' ..." % backup.id)
        update_backup(backup,
                      event_name="ERROR_HANDLING_START_TAR",
                      message="Taring failed dump")

        dump_dir = _backup_dump_dir_name(backup)
        failed_tar_filename = _failed_tar_file_name(backup)

        failed_dest = _failed_upload_file_dest(backup)
        # tar up
        self.backup_assistant.tar_backup(backup, dump_dir, failed_tar_filename)
        update_backup(backup,
                      event_name="ERROR_HANDLING_END_TAR",
                      message="Finished taring failed dump")

        # upload
        logger.info("Uploading tar for failed backup '%s' ..." % backup.id)
        update_backup(backup,
                      event_name="ERROR_HANDLING_START_UPLOAD",
                      message="Uploading failed dump tar")

        # upload failed tar file and allow overwriting existing
        target_reference = self.backup_assistant.upload_backup(backup, failed_tar_filename, backup.target,
                                                               destination_path=failed_dest)
        backup.target_reference = target_reference

        update_backup(backup, properties="targetReference",
                      event_name="ERROR_HANDLING_END_UPLOAD",
                      message="Finished uploading failed tar")

    ###########################################################################
    def dump_backup(self, backup, mongo_connector, database_name=None):

        update_backup(backup, event_name=EVENT_START_EXTRACT,
                      message="Dumping backup")

        # only mongo servers

        if not isinstance(mongo_connector, MongoServer):
            raise Exception("Connector '%s' is not a MongoServer" % mongo_connector)
        # dump the the server
        uri = mongo_connector.dump_uri()
        uri_wrapper = mongo_uri_tools.parse_mongo_uri(uri)
        if database_name and not uri_wrapper.database:
            if not uri.endswith("/"):
                uri += "/"
            uri += database_name

        # DUMP command
        destination = _backup_dump_dir_name(backup)

        dump_options = []
        # Add --journal for config server backup
        if mongo_connector.is_config_server():
            dump_options.append("--journal")

        # if its a server level backup then add forceTableScan and oplog
        uri_wrapper = mongo_uri_tools.parse_mongo_uri(uri)
        if not uri_wrapper.database:
            # add forceTableScan if specified
            if self.force_table_scan:
                dump_options.append("--forceTableScan")
            if mongo_connector.is_replica_member():
                dump_options.append("--oplog")

        # if mongo version is >= 2.4 and we are using admin creds then pass
        # --authenticationDatabase
        mongo_version = mongo_connector.get_mongo_version()
        if mongo_version >= MongoNormalizedVersion("2.4.0") :
            dump_options.extend([
                "--authenticationDatabase",
                "admin"
            ])

        # include users in dump if its a database dump and
        # mongo version is >= 2.6.0
        if (mongo_version >= MongoNormalizedVersion("2.6.0") and
                    database_name != None and
                    self.dump_users is not False):
            dump_options.append("--dumpDbUsersAndRoles")

        # apply overrides
        self._apply_dump_options_overrides(dump_options)

        log_file_name = _log_file_name(backup)
        # execute dump command
        dump_info = self.backup_assistant.dump_backup(backup, uri, destination, log_file_name, options=dump_options)
        if dump_info and "dumpCollectionCounts" in dump_info:
            backup.data_stats["dumpCollectionCounts"] = dump_info["dumpCollectionCounts"]

        update_backup(backup, properties="dataStats",
                      event_name=EVENT_END_EXTRACT,
                      message="Dump completed")

    ###########################################################################
    def _needs_new_member_selection(self, backup):
        """
          @Override
          If the backup has been dumped already then there is no need for
          selecting a new member
        """
        return not backup.is_event_logged(EVENT_END_EXTRACT)

    ###########################################################################
    def _needs_new_source_stats(self, backup):
        """
          @Override
          If the backup has been dumped already then there is no need for
          recording new source stats
        """
        return not backup.is_event_logged(EVENT_END_EXTRACT)

    ###########################################################################
    # Restore implementation
    ###########################################################################
    def _do_run_restore(self, restore):

        logger.info("Running dump restore '%s'" % restore.id)
        self.backup_assistant.create_task_workspace(restore)
        # download source backup tar
        if not restore.is_event_logged("END_DOWNLOAD_BACKUP"):
            self._download_source_backup(restore)

        if not restore.is_event_logged("END_EXTRACT_BACKUP"):
            # extract tar
            self._extract_source_backup(restore)

        try:

            if not restore.is_event_logged("END_RESTORE_DUMP"):
                # restore dump
                self._restore_dump(restore)
                self._validate_restore(restore)
                #self._upload_restore_log_file(restore)
        except RestoreError, e:
            #self._upload_restore_log_file(restore)
            raise


    ###########################################################################
    def _download_source_backup(self, restore):
        update_restore(restore, event_name="START_DOWNLOAD_BACKUP",
                       message="Download source backup file...")

        self.backup_assistant.download_restore_source_backup(restore)

        update_restore(restore, event_name="END_DOWNLOAD_BACKUP",
                       message="Source backup file download complete!")


    ###########################################################################
    def _extract_source_backup(self, restore):
        update_restore(restore, event_name="START_EXTRACT_BACKUP",
                       message="Extract backup file...")

        self.backup_assistant.extract_restore_source_backup(restore)

        update_restore(restore, event_name="END_EXTRACT_BACKUP",
                       message="Extract backup file completed!")

    ###########################################################################
    def _restore_dump(self, restore):

        file_reference = restore.source_backup.target_reference

        update_restore(restore, event_name="START_RESTORE_DUMP",
                       message="Running mongorestore...")

        # run mongoctl restore
        logger.info("Restoring using mongoctl restore")
        dump_dir = file_reference.file_name[: -4]



        # connect to the destination
        mongo_connector = self.get_restore_mongo_connector(restore)
        dest_uri = mongo_connector.restore_uri()

        dest_uri_wrapper = mongo_uri_tools.parse_mongo_uri(dest_uri)

        source_stats = restore.source_backup.source_stats
        source_mongo_version = source_stats and "version" in source_stats and \
                               MongoNormalizedVersion(source_stats["version"])
        dest_mongo_version = mongo_connector.get_mongo_version()

        # Delete admin system.user collection files if no_users_restore or restoring from 2.4 => 2.6
        exclude_admin_system_users = self.no_users_restore or source_mongo_version < VERSION_2_6 <= dest_mongo_version

        # Delete  system.user collection files if restoring from/to >=2.6
        exclude_system_users = source_mongo_version >= VERSION_2_6 and dest_mongo_version >= VERSION_2_6

        exclude_system_roles = self.no_roles_restore

        if dest_mongo_version >= VERSION_2_6:
            _grant_restore_role(mongo_connector)

        # append database name for destination uri if destination is a server
        # or a cluster
        # TODO this needs to be refactored where uri always include database
        # and BackupSource should include more info its a server or a cluster
        # i.e. credz are admin
        if not dest_uri_wrapper.database and restore.destination.database_name:
            dest_uri = "%s/%s" % (dest_uri, restore.destination.database_name)
            dest_uri_wrapper = mongo_uri_tools.parse_mongo_uri(dest_uri)

        source_database_name = restore.source_database_name
        if not source_database_name:
            if restore.source_backup.source.database_name:
                source_database_name =\
                    restore.source_backup.source.database_name
            else:
                stats = restore.source_backup.source_stats
                source_database_name = stats.get("databaseName")

        # map source/dest
        if source_database_name:
            if not dest_uri_wrapper.database:
                if not dest_uri.endswith("/"):
                    dest_uri += "/"
                dest_uri += source_database_name

        restore_options = []

        # append  --oplogReplay for cluster backups/restore
        if (not source_database_name and
            "repl" in restore.source_backup.source_stats):
            restore_options.append("--oplogReplay")

        # if mongo version is >= 2.4 and we are using admin creds then pass
        # --authenticationDatabase

        if (dest_mongo_version >= MongoNormalizedVersion("2.4.0") and
                isinstance(mongo_connector, (MongoServer, MongoCluster))) :
            restore_options.extend([
                "--authenticationDatabase",
                "admin"
            ])

        # include users in restore if its a database restore and
        # mongo version is >= 2.6.0
        if ((not self.no_roles_restore or not self.no_users_restore) and
                    dest_mongo_version >= VERSION_2_6 and source_database_name is not None):
            restore_options.append("--restoreDbUsersAndRoles")

        # stop on errors for 3.0 restores
        if dest_mongo_version >= VERSION_3_0:
            restore_options.append("--stopOnError")
            restore_options.extend(["--numParallelCollections", "1"])

        # additional restore options
        if self.no_index_restore:
            restore_options.append("--noIndexRestore")

        restore_options = self._apply_restore_options_overrides(restore_options)

        # execute dump command
        restore_info = self.backup_assistant.run_mongo_restore(
            restore, dest_uri, dump_dir, source_database_name,
            _restore_log_file_name(restore), _log_file_name(restore.source_backup),
            exclude_admin_system_users=exclude_admin_system_users,
            exclude_system_users=exclude_system_users,
            exclude_system_roles=exclude_system_roles,
            options=restore_options)

        if restore_info and "restoreCollectionCounts" in restore_info:
            restore.data_stats["restoreCollectionCounts"] = restore_info["restoreCollectionCounts"]

        update_restore(restore, properties="restoreCollectionCounts",
                       event_name="END_RESTORE_DUMP",
                       message="Restoring dump completed!")


    ###########################################################################
    def get_restore_mongo_connector(self, restore):
        logger.info("Selecting connector to run restore '%s'" % restore.id)
        destination_connector = restore.destination.get_connector()

        selected_connector = None
        if isinstance(destination_connector, MongoCluster):
            selected_connector = destination_connector.primary_member
        if isinstance(destination_connector, MongoServer) and destination_connector.is_primary():
            selected_connector = destination_connector

        if selected_connector:
            logger.info("Selected connector %s for restore '%s'" % (selected_connector.info(), restore.id))
            return selected_connector
        raise Exception("Cannot final a primary for restore connector %s (restore %s)" %
                        (destination_connector, restore.id))

    ###########################################################################
    def _validate_restore(self, restore):
        try:

            restore.data_stats["dumpCollectionCounts"] = read_dump_collection_counts(restore.source_backup)
            update_restore(restore, properties="dataStats",
                           event_name="READ_DUMP_COLLECTION_COUNTS",
                           message="Reading mongodump collection counts for validation")

            restore.data_stats["destinationCollectionCounts"] = self.get_destination_collection_counts(restore)
            update_restore(restore, properties="dataStats",
                           event_name="GET_DEST_COLLECTION_COUNTS",
                           message="Reading destination collection counts for validation")

            restore.valid = self._compare_all_collection_counts(restore)
            update_restore(restore, properties="valid",
                           event_name="SET_VALID",
                           message="setting valid to %s" % restore.valid)
        except Exception, ex:
            logger.exception("Error during validate restore '%s'" % restore.id)


    ###########################################################################
    def _compare_all_collection_counts(self, restore):
        src_db = restore.source_database_name or restore.source_backup.source.database_name
        dest_db = restore.destination.database_name

        c1 = self._comparable_collection_count(restore.data_stats.get("dumpCollectionCounts"), src_db=src_db, dest_db=dest_db)
        c2 = self._comparable_collection_count(restore.data_stats.get("restoreCollectionCounts"))
        c3 = self._comparable_collection_count(restore.data_stats.get("destinationCollectionCounts"))
        all_counts = [c1, c2, c3]

        # add backup dump counts if present
        if restore.source_backup.data_stats.get("dumpCollectionCounts"):
            c4 = self._comparable_collection_count(restore.source_backup.data_stats.get("dumpCollectionCounts"),
                                                   src_db=src_db, dest_db=dest_db)
            all_counts.append(c4)

        for cc in all_counts[1:]:
            if cc != all_counts[0]:
                return False

        return True

    ###########################################################################
    def _comparable_collection_count(self, collection_count, src_db=None, dest_db=None):
        comparable_count = {}

        if collection_count:
            for dbname in collection_count.keys():
                # skip admin
                if dbname in ["admin", "local"] or not collection_count.get(dbname):
                    continue

                sorted_db_count = sorted(collection_count[dbname], key=(lambda x : x["name"]))
                if src_db and dest_db and dbname == src_db and dbname !=dest_db:
                    compare_db = dest_db
                else:
                    compare_db = dbname

                comparable_count[compare_db] = sorted_db_count

        return comparable_count

    ###########################################################################
    def get_destination_collection_counts(self, restore):
        # connect to the destination
        mongo_connector = restore.destination.get_connector()
        return mongo_connector.get_collection_counts(only_for_db=restore.destination.database_name)

    ###########################################################################
    def _upload_restore_log_file(self, restore):
        log_file_path = self._get_restore_log_path(restore)
        logger.info("Uploading log file for %s to target" % restore.id)

        update_restore(restore, event_name="START_UPLOAD_LOG_FILE",
                       message="Upload log file to target")
        log_dest_path = _upload_restore_log_file_dest(restore)
        log_ref = restore.source_backup.target.put_file(log_file_path,
                                          destination_path= log_dest_path,
                                          overwrite_existing=True)

        restore.log_target_reference = log_ref

        update_restore(restore, properties="logTargetReference",
                       event_name="END_UPLOAD_LOG_FILE",
                       message="Log file upload completed!")

        logger.info("Upload log file for %s completed successfully!" %
                    restore.id)


    ###########################################################################
    def _apply_dump_options_overrides(self, dump_options):
        return self._apply_mongoctl_options_overrides(dump_options, self.dump_options_overrides)

    ###########################################################################
    def _apply_restore_options_overrides(self, restore_options):
        return self._apply_mongoctl_options_overrides(restore_options, self.restore_options_overrides)

    ###########################################################################
    def _apply_mongoctl_options_overrides(self, options, overrides):
        """

        :param options as list:
        :param overrides as list:
        :return applies overrides in the overrides list to the options list:
        """
        options = options or []
        overrides = overrides or []

        options_dict = _option_list_to_dict(options)
        overrides_dict = _option_list_to_dict(overrides)

        for name, val in overrides_dict.items():
            options_dict[name] = val

        # convert
        return _dict_to_option_list(options_dict)

###############################################################################
def _option_list_to_dict(options):
    """

    :param options:
    :return options as a dictionary:
    """
    options_dict = {}
    option_name = None

    for o in options:
        if o.startswith("-"):
            option_name = o
            options_dict[option_name] = []
            continue
        else:
            option_val = o
            options_dict[option_name].append(option_val)

    return options_dict

###############################################################################
def _dict_to_option_list(options_dict):
    """

    :param options_dict:
    :return options as list:
    """
    options = []
    for name, val in options_dict.items():
        options.append(name)
        options.extend(val)

    return options


###############################################################################
# Helpers
###############################################################################

def _log_file_name(backup):
    return "%s.log" % _backup_dump_dir_name(backup)

###############################################################################
def _tar_file_name(backup):
    return "%s.tgz" % _backup_dump_dir_name(backup)

###############################################################################
def _failed_tar_file_name(backup):
    return "FAILED_%s.tgz" % _backup_dump_dir_name(backup)

###############################################################################
def _backup_dump_dir_name(backup):
    # Temporary work around for backup names being a path instead of a single
    # name
    # TODO do the right thing
    return os.path.basename(backup.name)

###############################################################################
def _upload_file_dest(backup):
    return "%s.tgz" % backup.name

###############################################################################
def _upload_log_file_dest(backup):
    return "%s.log" % backup.name

###############################################################################
def _upload_restore_log_file_dest(restore):
    dest =  "%s.log" % restore.source_backup.name
    # append RESTORE as a prefix for the file name  + handle the case where
    # backup name is a path (as appose to just a file name)
    parts = dest.rpartition("/")
    return "%s%sRESTORE_%s" % (parts[0], parts[1], parts[2])

###############################################################################
def _failed_upload_file_dest(backup):
    dest =  "%s.tgz" % backup.name
    # append FAILED as a prefix for the file name  + handle the case where
    # backup name is a path (as appose to just a file name)
    parts = dest.rpartition("/")
    return "%s%sFAILED_%s" % (parts[0], parts[1], parts[2])

###############################################################################
def _restore_log_file_name(restore):
    return "RESTORE_%s.log" % _backup_dump_dir_name(restore.source_backup)

###############################################################################
# CloudBlockStorageStrategy
###############################################################################
class CloudBlockStorageStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        BackupStrategy.__init__(self)
        self._constituent_name_scheme = None
        self._constituent_description_scheme = None
        self._use_fsynclock = True

    ###########################################################################
    @property
    def constituent_name_scheme(self):
        return self._constituent_name_scheme

    @constituent_name_scheme.setter
    def constituent_name_scheme(self, val):
        self._constituent_name_scheme = val

    ###########################################################################
    @property
    def constituent_description_scheme(self):
        return self._constituent_description_scheme

    @constituent_description_scheme.setter
    def constituent_description_scheme(self, val):
        self._constituent_description_scheme = val

    ###########################################################################
    def do_backup_mongo_connector(self, backup, mongo_connector):
        self._snapshot_backup(backup, mongo_connector)

    ###########################################################################
    def is_use_fsynclock(self):
        # Always use suspend io unless explicitly set to False
        return self.use_fsynclock is None or self.use_fsynclock

    ###########################################################################
    def is_use_suspend_io(self):
        # Always use suspend io unless explicitly set to False
        return ((self.use_suspend_io is None or self.use_suspend_io) and
                self.is_use_fsynclock())

    ###########################################################################
    def _snapshot_backup(self, backup, mongo_connector):
        logger.info("Running snapshot backup '%s'..." % backup.id)

        address = mongo_connector.address

        logger.info("Getting cloud block storage object for snapshot backup '%s'..." % backup.id)
        cbs = self.get_backup_cbs(backup, mongo_connector)

        # validate
        if not cbs:
            msg = ("Cannot run a block storage snapshot backup for backup '%s'"
                   ".Backup source does not have a cloudBlockStorage "
                   "configured for address '%s'" % (backup.id, address))
            raise NoCloudBlockStorageFoundError(msg)

        update_backup(backup, event_name="START_BLOCK_STORAGE_SNAPSHOT",
                      message="Starting snapshot backup...")

        # kickoff the snapshot if it was not kicked off before or if the current snapshot is in error state
        if (not backup.is_event_logged("END_KICKOFF_SNAPSHOT") or
            (backup.target_reference and backup.target_reference.status == SnapshotStatus.ERROR)):
            self._kickoff_snapshot(backup, mongo_connector, cbs)

        # hook for doing things after a snapshot was already kicked off
        self._post_snapshot_kickoff(backup, mongo_connector, cbs)

        # wait until snapshot is completed or error (sleep time is 1 minute)
        wait_status = [SnapshotStatus.COMPLETED, SnapshotStatus.ERROR]
        self._wait_for_snapshot_status(backup, cbs, wait_status,
                                       sleep_time=60)

        if backup.target_reference.status == SnapshotStatus.COMPLETED:
            logger.info("Successfully completed backup '%s' snapshot" %
                        backup.id)
            msg = "Snapshot completed successfully"
            update_backup(backup, event_name="END_BLOCK_STORAGE_SNAPSHOT",
                          message=msg)
        else:
            raise SnapshotDidNotSucceedError("Snapshot did not complete successfully. Snapshot status became '%s'" %
                                             backup.target_reference.status)

    ###########################################################################
    def _kickoff_snapshot(self, backup, mongo_connector, cbs):
        """
        Creates the snapshot and waits until it is kicked off (state pending)
        :param backup:
        :param cbs:
        :return:
        """
        # ensure that the instance has is unlocked and resumed if it was a rescheduled one
        self._ensure_unlocked_and_resumed(backup, mongo_connector, cbs)

        # if there is an existing snapshot then delete it before creating the new one
        self._delete_existing_snapshot(backup, cbs)

        use_fysnclock = (self.backup_mode == BackupMode.ONLINE and
                         mongo_connector.is_online() and
                         self.is_use_fsynclock())
        use_suspend_io = self.is_use_suspend_io() and use_fysnclock
        fsync_unlocked = False

        resumed_io = False

        need_to_resume_balancer = False
        balancer_resumed = False
        try:
            update_backup(backup, event_name="START_KICKOFF_SNAPSHOT",
                          message="Kicking off snapshot")

            # sharded connectors: Stop balancer before snapshot as needed
            # also make sure that the balancer was not active during snapshot
            # kick off

            if isinstance(mongo_connector, ShardedClusterConnector):
                if mongo_connector.is_balancer_active():
                    need_to_resume_balancer = True
                self._stop_balancer(backup, mongo_connector)

                # monitor balancer during kickoff window
                mongo_connector.start_balancer_activity_monitor()

            # run fsync lock
            if use_fysnclock:
                self._fsynclock(backup, mongo_connector)
            else:
                msg = ("Snapshot Backup '%s' will be taken WITHOUT "
                       "locking database and IO!" % backup.id)
                logger.warning(msg)
                update_backup(backup, event_type=EventType.WARNING,
                              event_name="NOT_LOCKED",
                              message=msg)

            # suspend io
            if use_suspend_io:
                self._suspend_io(backup, mongo_connector, cbs)

            # create the snapshot
            self._create_snapshot(backup, cbs)

            # wait until snapshot is pending or completed or error
            self._wait_for_pending_status(backup, cbs)

            # resume io/unlock

            if use_suspend_io:
                self._resume_io(backup, mongo_connector, cbs)
                resumed_io = True

            if use_fysnclock:
                self._fsyncunlock(backup, mongo_connector)
                fsync_unlocked = True

            # sharded connectors: Resume balancer after snapshot as needed
            if isinstance(mongo_connector, ShardedClusterConnector):
                # check that the balancer was not active during kickoff
                mongo_connector.stop_balancer_activity_monitor()
                if mongo_connector.balancer_active_during_monitor():
                    logger.error("Balancer detected to be active"
                                 " during kickoff for '%s'" % mongo_connector)
                    raise BalancerActiveError("Balancer detected to be active"
                                              " during kickoff")
                if need_to_resume_balancer:
                    self._resume_balancer(backup, mongo_connector)
                    balancer_resumed = True

            update_backup(backup, event_name="END_KICKOFF_SNAPSHOT",
                          message="Snapshot kicked off successfully!")

        except Exception, ex:
            msg = "Snapshot kickoff error: %s" % ex
            logger.exception(msg)
            update_backup(backup, message=msg, event_type=EventType.ERROR)
            raise

        finally:
            logger.info("Do post snapshot kickoff necessary cleanup..")
            try:
                # resume io/unlock as needed
                if use_suspend_io and not resumed_io:
                    logger.info("It seems that the IO was suspended and has"
                                " not been resumed. Resuming IO...")
                    self._resume_io(backup, mongo_connector, cbs)
            except Exception, ex:
                logger.exception("Snapshot kickoff cleanup error: Resume "
                                 "IO Error %s: " % ex)

            try:
                # resume io/unlock as needed
                if use_suspend_io and not resumed_io:
                    logger.info("It seems that the IO was suspended and has"
                                " not been resumed. Resuming IO...")
                    self._resume_io(backup, mongo_connector, cbs)
            except Exception, ex:
                logger.exception("Snapshot kickoff cleanup error: Resume "
                                 "IO Error %s: " % ex)

            try:
                # resume io/unlock as needed
                if use_fysnclock and not fsync_unlocked:
                    self._fsyncunlock(backup, mongo_connector)
            except Exception, ex:
                logger.exception("Snapshot kickoff cleanup error: fsyncunlock "
                                 "IO Error %s: " % ex)

            try:
                if need_to_resume_balancer and not balancer_resumed:
                    self._resume_balancer(backup, mongo_connector)
            except Exception, ex:
                logger.exception("Snapshot kickoff cleanup error: resume "
                                 "balancer Error: %s" % ex)

    ###########################################################################
    def _post_snapshot_kickoff(self, backup, mongo_connector, cbs):
        pass

    ###########################################################################
    def _delete_existing_snapshot(self, backup, cbs):
        # if this is a rescheduled backup with an existing snapshot then
        # delete existing one since we are creating a new one
        if backup.target_reference:
            logger.info("Detected an existing snapshot for backup '%s'. "
                        "Deleting it before creating new one" %
                        backup.id)
            cbs.delete_snapshot(backup.target_reference)


    ###########################################################################
    def _ensure_unlocked_and_resumed(self, backup, mongo_connector, cbs):

        last_suspend = backup.get_last_event_entry(BackupEventNames.SUSPEND_IO)
        last_resume = backup.get_last_event_entry(BackupEventNames.RESUME_IO)

        if last_suspend and (not last_resume or last_suspend.date > last_resume.date):
            logger.info("Detected io suspending for backup '%s'. Issuing an resume" % backup.id)
            self._resume_io(backup, mongo_connector, cbs)

        last_lock = backup.get_last_event_entry(BackupEventNames.FSYNCLOCK)
        last_unlock = backup.get_last_event_entry(BackupEventNames.FSYNCUNLOCK)

        if last_lock and (not last_unlock or last_lock.date >= last_unlock.date):
            logger.info("Detected fsynclock for backup '%s'. Issuing an unlock" % backup.id)
            self._fsyncunlock(backup, mongo_connector)

    ###########################################################################
    def _create_snapshot(self, backup, cbs):

        logger.info("Initiating block storage snapshot for backup '%s'" %
                    backup.id)

        # Refresh backup name/description
        self._set_backup_name_and_desc(backup, update=True)
        if isinstance(cbs, CompositeBlockStorage):
            self._create_composite_snapshot(backup, cbs)
        else:
            self._create_single_snapshot(backup, cbs)

    ###########################################################################
    def _create_single_snapshot(self, backup, cbs):
        update_backup(backup, event_name="START_CREATE_SNAPSHOT",
                      message="Creating snapshot")

        snapshot_ref = cbs.create_snapshot(backup.name, backup.description)

        # set sourceWasLocked field
        snapshot_ref.source_was_locked = backup.is_event_logged(BackupEventNames.FSYNCLOCK_END)

        backup.target_reference = snapshot_ref

        update_backup(backup, properties="targetReference",
                      event_name="END_CREATE_SNAPSHOT",
                      message="Snapshot created successfully")

    ###########################################################################
    def _create_composite_snapshot(self, backup, cbs):
        count = len(cbs.constituents)
        msg = "Creating composite snapshot (composed of %s snapshots)" % count
        logger.info("%s, backup id '%s' " % (msg, backup.id))

        update_backup(backup, event_name="START_CREATE_SNAPSHOT",
                      message=msg)

        name_template = self.constituent_name_scheme or backup.name
        desc_template = (self.constituent_description_scheme or
                         backup.description)

        name_template = self._generate_name(backup, name_template)
        desc_template = self._generate_name(backup, desc_template)

        snapshot_ref = cbs.create_snapshot(name_template, desc_template)

        # set sourceWasLocked field
        snapshot_ref.source_was_locked = backup.is_event_logged(BackupEventNames.FSYNCLOCK_END)

        backup.target_reference = snapshot_ref



        msg = ("Composite snapshot created successfully "
               "(composed of %s snapshots)" % count)

        logger.info("%s, backup id '%s' " % (msg, backup.id))

        update_backup(backup, properties="targetReference",
                      event_name="END_CREATE_SNAPSHOT",
                      message=msg)
    ###########################################################################
    def _wait_for_pending_status(self, backup, cbs):

        # wait until snapshot is pending or completed or error
        wait_status = [SnapshotStatus.PENDING, SnapshotStatus.COMPLETED,
                       SnapshotStatus.ERROR]
        self._wait_for_snapshot_status(backup, cbs, wait_status)

    ###########################################################################
    def _wait_for_snapshot_status(self, backup, cbs, wait_status,
                                  sleep_time=5):
        msg = ("Waiting for backup '%s' snapshot status to be in %s" %
               (backup.id, wait_status))
        logger.info(msg)
        update_backup(backup, message=msg)

        # wait until snapshot is completed and keep target ref up to date
        snapshot_ref = backup.target_reference
        wait_status = listify(wait_status)
        while snapshot_ref.status not in wait_status:
            logger.debug("Checking updates for backup '%s' snapshot" %
                         backup.id)
            new_snapshot_ref = cbs.check_snapshot_updates(snapshot_ref)
            if new_snapshot_ref:
                logger.info("Detected updates for backup '%s' snapshot " %
                            backup.id)
                diff = snapshot_ref.diff(new_snapshot_ref)
                logger.info("Diff: \n%s" % document_pretty_string(diff))
                snapshot_ref = new_snapshot_ref
                backup.target_reference = snapshot_ref
                update_backup(backup, properties="targetReference")

            time.sleep(sleep_time)


    ###########################################################################
    def _needs_new_source_stats(self, backup):
        """
          @Override
          If the backup has been snapshoted already then there is no need for
          recording new source stats
        """
        return not backup.is_event_logged("END_CREATE_SNAPSHOT")

    ###########################################################################
    def _needs_new_member_selection(self, backup):
        """
          @Override
          If the backup has been snapshoted already then there is no need for
          selecting a new member
        """
        return not backup.is_event_logged("END_CREATE_SNAPSHOT")

    ###########################################################################
    def get_backup_cbs(self, backup, mongo_connector):
        #TODO need to reconsider this
        return self.get_backup_source_cbs(backup.source, mongo_connector)

    ###########################################################################
    def get_backup_source_cbs(self, source, mongo_connector):
        return source.get_block_storage_by_connector(mongo_connector)

    ###########################################################################
    def _do_run_restore(self, restore):
        raise RuntimeError("Restore for cloud block storage not support yet")

    ###########################################################################
    def to_document(self, display_only=False):
        doc = BackupStrategy.to_document(self, display_only=display_only)
        doc.update({
            "_type": "CloudBlockStorageStrategy"
        })

        if self.constituent_name_scheme:
            doc["constituentNameScheme"] = self.constituent_name_scheme

        if self.constituent_description_scheme:
            doc["constituentDescriptionScheme"] = \
                self.constituent_description_scheme

        return doc

###############################################################################
# Hybrid Strategy Class
###############################################################################
DUMP_MAX_DATA_SIZE = 50 * 1024 * 1024 * 1024

class HybridStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        BackupStrategy.__init__(self)
        self._dump_strategy = DumpStrategy()
        self._cloud_block_storage_strategy = CloudBlockStorageStrategy()
        self._predicate = DataSizePredicate()
        self._selected_strategy_type = None

    ###########################################################################
    @property
    def dump_strategy(self):
        return self._dump_strategy

    @dump_strategy.setter
    def dump_strategy(self, val):
        self._dump_strategy = val

    ###########################################################################
    @property
    def predicate(self):
        return self._predicate

    @predicate.setter
    def predicate(self, val):
        self._predicate = val

    ###########################################################################
    @property
    def selected_strategy_type(self):
        return self._selected_strategy_type

    @selected_strategy_type.setter
    def selected_strategy_type(self, val):
        self._selected_strategy_type = val

    ###########################################################################
    @property
    def cloud_block_storage_strategy(self):
        return self._cloud_block_storage_strategy

    @cloud_block_storage_strategy.setter
    def cloud_block_storage_strategy(self, val):
        self._cloud_block_storage_strategy = val

    ###########################################################################
    def _do_run_backup(self, backup):
        mongo_connector = self.get_backup_mongo_connector(backup)
        # TODO Maybe tag backup with selected strategy so that we dont need
        # to re-determine that again

        selected_strategy = self.select_strategy(backup, mongo_connector)
        self._run_backup_for_selected_strategy(backup, mongo_connector, selected_strategy)

    ###########################################################################
    def _run_backup_for_selected_strategy(self, backup, mongo_connector, selected_strategy):
        selected_strategy.backup_mongo_connector(backup, mongo_connector)

    ###########################################################################
    def select_strategy(self, backup, mongo_connector):
        if not self.selected_strategy_type:

            # if the connector was offline and its allowed then use cbs
            if (self.backup_mode == BackupMode.OFFLINE or
                    (self.allow_offline_backups and
                     not mongo_connector.is_online())):
                selected_strategy = self.cloud_block_storage_strategy
            else:
                selected_strategy = self.predicate.get_best_strategy(
                    self, backup, mongo_connector)


        elif self.selected_strategy_type == self.dump_strategy.type_name:
            selected_strategy = self.dump_strategy
        else:
            selected_strategy = self.cloud_block_storage_strategy

        # set defaults and save back
        self._set_default_settings(selected_strategy)
        self.selected_strategy_type = selected_strategy.type_name

        logger.info("Strategy initialized to for backup %s. "
                    "Saving it back to the backup: %s" %
                    (backup.id, self))

        backup.strategy = self
        update_backup(backup, properties="strategy",
                      event_name="SELECT_STRATEGY",
                      message="Initialize strategy config")

        return selected_strategy

    ###########################################################################
    def _set_default_settings(self, strategy):
        strategy.member_preference = self.member_preference
        strategy.backup_mode = self.backup_mode

        strategy.ensure_localhost = self.ensure_localhost
        strategy.max_data_size = self.max_data_size
        strategy.use_suspend_io = self.use_suspend_io
        strategy.allow_offline_backups = self.allow_offline_backups
        strategy.max_lag_seconds = self.max_lag_seconds
        strategy.disable_source_stats = self.disable_source_stats

        if self.use_fsynclock is not None:
            strategy.use_fsynclock = self.use_fsynclock

        strategy.backup_name_scheme = \
            (strategy.backup_name_scheme or
             self.backup_name_scheme)

        strategy.backup_description_scheme = \
            (strategy.backup_description_scheme or
             self.backup_description_scheme)

        strategy.backup_assistant = self.backup_assistant

        # restore settings
        strategy.no_index_restore = self.no_index_restore
        strategy.no_users_restore = self.no_users_restore
        strategy.no_roles_restore = self.no_roles_restore

    ###########################################################################
    def _do_run_restore(self, restore):

        if restore.source_backup.is_event_logged(EVENT_END_EXTRACT):
            selected_strategy = self.dump_strategy
        else:
            selected_strategy = self.cloud_block_storage_strategy

        # set defaults and save back
        self._set_default_settings(selected_strategy)

        return selected_strategy._do_run_restore(restore)

    ###########################################################################
    def _set_backup_name_and_desc(self, backup):
        """
         Do nothing so that the selected strategy will take care of that
         instead
        """

    ###########################################################################
    def _needs_new_member_selection(self, backup):
        """
          @Override
          If the backup has been dumped/snapshoted already then there is no
          need for selecting a new member
        """
        ds = self.dump_strategy
        cs = self.cloud_block_storage_strategy
        return (ds._needs_new_member_selection(backup) and
                cs._needs_new_member_selection(backup))

    ###########################################################################
    def _needs_new_source_stats(self, backup):
        """
          @Override
          If the backup has been dumped or snapshoted already then there is
          no need for re-recording source stats
        """
        ds = self.dump_strategy
        cs = self.cloud_block_storage_strategy
        return (ds._needs_new_source_stats(backup) and
                cs._needs_new_source_stats(backup))

    #################################################################################
    def to_document(self, display_only=False):
        doc =  BackupStrategy.to_document(self, display_only=display_only)
        doc.update({
            "_type": "HybridStrategy",
            "dumpStrategy":
                self.dump_strategy.to_document(display_only=display_only),

            "cloudBlockStorageStrategy":
                self.cloud_block_storage_strategy.to_document(display_only=
                                                               display_only),

            "predicate": self.predicate.to_document(display_only=display_only),
            "selectedStrategyType": self.selected_strategy_type
        })

        return doc

###############################################################################
# HybridStrategyPredicate
###############################################################################
class HybridStrategyPredicate(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def get_best_strategy(self, hybrid_strategy, backup, mongo_connector):
        """
            Returns the best strategy to be used for running the specified
            backup
            Must be overridden by subclasses
        """
        pass

###############################################################################
# DataSizePredicate
###############################################################################
class DataSizePredicate(HybridStrategyPredicate):

    ###########################################################################
    def __init__(self):
        self._dump_max_data_size = DUMP_MAX_DATA_SIZE

    ###########################################################################
    def get_best_strategy(self, hybrid_strategy, backup, mongo_connector):
        """
            Returns the best strategy to be used for running the specified
            backup
            Must be overridden by subclasses
        """
        logger.info("Selecting best strategy for backup '%s " % backup.id)

        data_size = self._get_backup_source_data_size(backup, mongo_connector)
        logger.info("Selecting best strategy for backup '%s', dataSize=%s, "
                    "dump max data size=%s" %
                    (backup.id, data_size, self.dump_max_data_size))

        if data_size < self.dump_max_data_size:
            logger.info("Selected dump strategy since dataSize %s is less"
                        " than dump max size %s" %
                        (data_size, self.dump_max_data_size))
            return hybrid_strategy.dump_strategy
        else:
            # if there is no cloud block storage for the selected connector
            # then choose dump and warn
            address = mongo_connector.address
            cbs_strategy = hybrid_strategy.cloud_block_storage_strategy
            block_storage = cbs_strategy.get_backup_cbs(backup,
                                                        mongo_connector)
            if block_storage is None:
                logger.warning("HybridStrategy: No cloud block storage found "
                               "for '%s'. Using dump strategy ..." % address)
                return hybrid_strategy.dump_strategy

            logger.info("Selected cloud block storage strategy since "
                        "dataSize %s is more than dump max size %s" %
                        (data_size, self.dump_max_data_size))
            return hybrid_strategy.cloud_block_storage_strategy

    ###########################################################################
    @property
    def dump_max_data_size(self):
        return self._dump_max_data_size

    @dump_max_data_size.setter
    def dump_max_data_size(self, val):
        self._dump_max_data_size = val

    ###########################################################################
    def _get_backup_source_data_size(self, backup, mongo_connector):
        database_name = backup.source.database_name
        logger.info("Computing dataSize for backup '%s', connector %s" %
                    (backup.id, mongo_connector.info()))
        stats = mongo_connector.get_stats(only_for_db=database_name)

        return stats["dataSize"]

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "DataSizePredicate",
            "dumpMaxDataSize":self.dump_max_data_size
        }


###############################################################################
# EbsVolumeStorageStrategy
###############################################################################
class EbsVolumeStorageStrategy(CloudBlockStorageStrategy):
    """
        Adds ebs specific features like sharing snapshots
    """
    ###########################################################################
    def __init__(self):
        CloudBlockStorageStrategy.__init__(self)
        self._share_users = None
        self._share_groups = None

    ###########################################################################
    def _post_snapshot_kickoff(self, backup, mongo_connector, cbs):
        """
            Override!
        """
        # call super method
        suber = super(EbsVolumeStorageStrategy, self)
        suber._post_snapshot_kickoff(backup, mongo_connector, cbs)
        snapshot_ref = backup.target_reference
        if snapshot_ref.status in [SnapshotStatus.PENDING, SnapshotStatus.COMPLETED]:
            logger.info("Checking if snapshot backup '%s' is configured to be "
                        "shared" % backup.id)
            is_sharing = self.share_users or self.share_groups

            if is_sharing:
                share_snapshot_backup(backup, cbs, user_ids=self.share_users,
                                      groups=self.share_groups)
            else:
                logger.info("Snapshot backup '%s' not configured to be "
                            "shared" % backup.id)

    ###########################################################################
    @property
    def share_users(self):
        return self._share_users

    @share_users.setter
    def share_users(self, val):
        self._share_users = val

    ###########################################################################
    @property
    def share_groups(self):
        return self._share_groups

    @share_groups.setter
    def share_groups(self, val):
        self._share_groups = val

    ###########################################################################
    def to_document(self, display_only=False):
        suber = super(EbsVolumeStorageStrategy, self)
        doc = suber.to_document(display_only=display_only)
        doc.update({
            "_type": "EbsVolumeStorageStrategy",
            "shareUsers": self.share_users,
            "shareGroups":self.share_groups
        })

        return doc

###############################################################################
@robustify(max_attempts=5, retry_interval=5,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def share_snapshot_backup(backup, cbs, user_ids=None, groups=None):
    msg = ("Sharing snapshot backup '%s' with users:%s, groups:%s " %
           (backup.id, user_ids, groups))
    logger.info(msg)

    target_ref = backup.target_reference

    # encrypted snapshots are not sharable. Log warning...
    if is_snapshot_volume_encrypted(target_ref):
        msg = "Will not share snapshot backup '%s' because the volume is encrypted" % backup.id
        logger.warning(msg)

        update_backup(backup, event_type=EventType.WARNING,
                      event_name="NO_ENCRYPTED_EBS_SHARING",
                      message=msg)
    else:
        target_ref = cbs.share_snapshot(target_ref, user_ids=user_ids, groups=groups)
        backup.target_reference = target_ref

        update_backup(backup, properties="targetReference",
                      event_name="SHARE_SNAPSHOT",
                      message=msg)

        logger.info("Snapshot backup '%s' shared successfully!" %
                    backup.id)


###############################################################################
def backup_format_bindings(backup):
    warning_keys = backup_warning_keys(backup)
    warning_keys_str = "".join(warning_keys) if warning_keys else ""
    return {
        "warningKeys":  warning_keys_str
    }

###############################################################################
def backup_warning_keys(backup):
    return set(map(lambda log_event: log_event.name, backup.get_warning_logs()))


###############################################################################
def _grant_restore_role(connector):
    logger.info("Check if we granting restore role is needed for %s" %
                connector)
    if isinstance(connector, MongoServer):
        admin_db = connector.admin_db
        user = connector._uri_wrapper.username
    elif isinstance(connector, MongoCluster):
        admin_db = connector.primary_member.admin_db
        user = connector.primary_member._uri_wrapper.username
    else:
        logger.info("restore role is NOT needed for %s. Skipping..." %
                    connector)
        return

    logger.info("Granting restore role for %s (user %s)..." %
                (connector, user))

    # construct grant role command
    roles = [
        {
            "role": "restore", "db": "admin"
        }
    ]

    role_cmd = SON([("grantRolesToUser", user), ("roles", roles)])

    logger.info("Executing db command '%s'" % role_cmd)

    admin_db.command(role_cmd)

    logger.info("restore role granted successfully!")

###############################################################################
def read_dump_collection_counts(backup):
    """
    reads the collection counts from the dump log
    :param backup:
    :return:
    """
    logger.info("Reading dump collection counts for backup from dump log'%s'" % backup.id)
    collection_counts = {}
    log_ref = backup.log_target_reference
    for line in backup.target.stream_file(log_ref):
        dbname, collection_name, count = process_dump_line_collection_counts(line)
        if dbname:
            if dbname not in collection_counts:
                collection_counts[dbname] = []
            collection_counts[dbname].append({
                "name": collection_name,
                "count": count
            })

    return collection_counts

###############################################################################
def process_dump_line_collection_counts(line):
    """
    :param line:
    :return: dbname, collection_name, doc count if the line is a "done dumping" line, else None,None,None
    """

    m = re.search('done dumping ([^.]*)\.([^ ]*) \(([0-9]*) document', line)
    if m:
        return m.groups()[0], m.groups()[1], int(m.groups()[2])
    else:
        return None, None, None

