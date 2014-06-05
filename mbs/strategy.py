__author__ = 'abdul'


import os
import time

import shutil


from mbs import get_mbs


from persistence import update_backup, update_restore
from mongo_utils import (MongoCluster, MongoServer,
                         MongoNormalizedVersion, build_mongo_connector)

from date_utils import timedelta_total_seconds, date_now

from subprocess import CalledProcessError
from errors import *
from utils import (which, ensure_dir, execute_command, execute_command_wrapper,
                   listify)

from source import CompositeBlockStorage

from target import (
    CBS_STATUS_PENDING, CBS_STATUS_COMPLETED, CBS_STATUS_ERROR,
    multi_target_upload_file, CloudBlockStorageSnapshotReference,
    EbsSnapshotReference, LVMSnapshotReference
)


from task import EVENT_TYPE_WARNING
from robustify.robustify import robustify
from naming_scheme import *

###############################################################################
# CONSTANTS
###############################################################################

# max number of retries
MAX_NO_RETRIES = 3

EVENT_START_EXTRACT = "START_EXTRACT"
EVENT_END_EXTRACT = "END_EXTRACT"
EVENT_START_ARCHIVE = "START_ARCHIVE"
EVENT_END_ARCHIVE = "END_ARCHIVE"
EVENT_START_UPLOAD = "START_UPLOAD"
EVENT_END_UPLOAD = "END_UPLOAD"

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
# LOGGER
###############################################################################

logger = mbs_logging.logger

###############################################################################
def _is_task_reschedulable( task, exception):
        return (task.try_count < MAX_NO_RETRIES and
                is_exception_retriable(exception))

###############################################################################
# BackupStrategy Classes
###############################################################################
class BackupStrategy(MBSObject):

    ###########################################################################
    def __init__(self):
        MBSObject.__init__(self)
        self._member_preference = MemberPreference.BEST
        self._ensure_localhost = False
        self._max_data_size = None
        self._backup_name_scheme = None
        self._backup_description_scheme = None

        self._use_fsynclock = None
        self._use_suspend_io = None
        self._allow_offline_backups = None
        self._backup_mode = BackupMode.ONLINE

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
    def is_use_suspend_io(self):
        return False

    ###########################################################################
    def run_backup(self, backup):
        try:
            self._do_run_backup(backup)
        except Exception, e:
            # set reschedulable
            backup.reschedulable = _is_task_reschedulable(backup, e)
            update_backup(backup, properties="reschedulable")
            raise

    ###########################################################################
    def _do_run_backup(self, backup):
        mongo_connector = self.get_backup_mongo_connector(backup)
        self.backup_mongo_connector(backup, mongo_connector)

    ###########################################################################
    def get_backup_mongo_connector(self, backup):
        logger.info("Selecting connector to run backup '%s'" % backup.id)
        connector = build_mongo_connector(backup.source.uri)
        if isinstance(connector, MongoCluster):
            connector = self._select_backup_cluster_member(backup, connector)

        self._validate_connector(backup, connector)

        return connector

    ###########################################################################
    def _validate_connector(self, backup, connector):

        logger.info("Validate selected connector '%s'..." % connector)

        logger.info("1- validating connectivity to '%s'..." % connector)
        if not connector.is_online():
            logger.info("'%s' is offline" % connector)
            if self.allow_offline_backups:
                logger.info("allowOfflineBackups is set to true so its all "
                            "good")
                self._set_backup_mode(backup, BackupMode.OFFLINE)
            elif self.backup_mode == BackupMode.OFFLINE:
                msg = "Selected connector '%s' is offline" % connector
                raise NoEligibleMembersFound(backup.source.uri, msg=msg)
        else:
            logger.info("Connector '%s' is online! Yay!" % connector)

        logger.info("Validating selected connector '%s' against member "
                    "preference '%s' for backup '%s'" %
                    (connector, self.member_preference, backup.id))

        if self.member_preference == MemberPreference.SECONDARY_ONLY:
            if not connector.is_secondary():
                msg = "Selected connector '%s' is not a secondary" % connector
                raise NoEligibleMembersFound(backup.source.uri, msg=msg)

        if (self.member_preference == MemberPreference.PRIMARY_ONLY and
                not connector.is_primary()):
            msg = "Selected connector '%s' is not a primary" % connector
            raise NoEligibleMembersFound(backup.source.uri, msg=msg)

        if (self.member_preference == MemberPreference.NOT_PRIMARY and
                connector.is_primary()):
            msg = "Selected connector '%s' is a Primary" % connector
            raise NoEligibleMembersFound(backup.source.uri, msg=msg)

        logger.info("Member preference validation for backup '%s' passed!" %
                    backup.id)

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
    def _needs_new_member_selection(self, backup):
        """
            Needs to be implemented by subclasses
        """
        return True

    ###########################################################################
    def get_mongo_connector_used_by(self, backup):
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

    ###########################################################################
    def _select_new_cluster_member(self, backup, mongo_cluster):

        source = backup.source

        # compute max lag
        if backup.plan:
            max_lag_seconds = backup.plan.schedule.max_acceptable_lag(
                                    backup.plan_occurrence)
        else:
            # One Off backup : no max lag!
            max_lag_seconds = 0

        # find a server to dump from

        primary_member = mongo_cluster.primary_member
        selected_member = None
        # dump from best secondary if configured and found
        if ((self.member_preference == MemberPreference.BEST and
             backup.try_count < MAX_NO_RETRIES) or
            (self.member_preference == MemberPreference.SECONDARY_ONLY and
             backup.try_count <= MAX_NO_RETRIES)):

            best_secondary = mongo_cluster.get_best_secondary(max_lag_seconds=
                                                               max_lag_seconds)

            if best_secondary:
                selected_member = best_secondary

                # FAIL if best secondary was not a P0 within max_lag_seconds
                # if cluster has any P0
                if (max_lag_seconds and mongo_cluster.has_p0s() and
                            best_secondary.priority != 0):
                    msg = ("No eligible p0 secondary found within max lag '%s'"
                           " for cluster '%s'" % (max_lag_seconds,
                                                  mongo_cluster))
                    raise NoEligibleMembersFound(source.uri, msg=msg)

                # log warning if secondary is too stale
                if best_secondary.is_too_stale():
                    logger.warning("Backup '%s' will be extracted from a "
                                   "too stale member!" % backup.id)

                    msg = ("Warning! The dump will be extracted from a too "
                           "stale member")
                    update_backup(backup, event_type=EVENT_TYPE_WARNING,
                                  event_name="USING_TOO_STALE_WARNING",
                                  message=msg)


        if (not selected_member and
            self.member_preference in [MemberPreference.BEST, MemberPreference.PRIMARY_ONLY]):
            # otherwise dump from primary if primary ok or if this is the
            # last try. log warning because we are dumping from a primary
            selected_member = primary_member
            logger.warning("Backup '%s' will be extracted from the "
                           "primary!" % backup.id)

            msg = "Warning! The dump will be extracted from the  primary"
            update_backup(backup, event_type=EVENT_TYPE_WARNING,
                          event_name="USING_PRIMARY_WARNING",
                          message=msg)

        if selected_member:
            return selected_member
        else:
            # error out
            raise NoEligibleMembersFound(source.uri)

    ###########################################################################
    def backup_mongo_connector(self, backup, mongo_connector):

        # ensure local host if specified
        if self.ensure_localhost and not mongo_connector.is_local():
            details = ("Source host for dump source '%s' is not running "
                       "locally and strategy.ensureLocalHost is set to true" %
                       mongo_connector)
            raise BackupNotOnLocalhost(msg="Error while attempting to dump",
                                       details=details)

        # record stats
        if not backup.source_stats or self._needs_new_source_stats(backup):
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
        except Exception, e:
            if is_connection_exception(e) and self.allow_offline_backups:
                # switch to offline mode
                self._set_backup_mode(backup, BackupMode.OFFLINE)
            else:
                raise

    ###########################################################################
    def _set_backup_mode(self, backup, mode):
        """
            sets/persists specified backup mode
        """
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
        duration = timedelta_total_seconds(date_now() - backup.start_date)
        if backup.source_stats and backup.source_stats.get("dataSize"):
            size_mb = float(backup.source_stats["dataSize"]) / (1024 * 1024)
            rate = size_mb/duration
            rate = round(rate, 2)
            if rate:
                backup.backup_rate_in_mbps = rate
                # save changes
                update_backup(backup, properties="backupRateInMBPS")

    ###########################################################################
    def cleanup_backup(self, backup):

        # delete the temp dir
        workspace = backup.workspace
        logger.info("Cleanup: deleting workspace dir %s" % workspace)
        update_backup(backup, event_name="CLEANUP", message="Running cleanup")

        try:

            if os.path.exists(workspace):
                shutil.rmtree(workspace)
            else:
                logger.error("workspace dir %s does not exist!" % workspace)
        except Exception, e:
            logger.error("Cleanup error for task '%s': %s" % (backup.id, e))

    ###########################################################################
    def run_restore(self, restore):
        try:
            self._do_run_restore(restore)
            self._compute_restore_destination_stats(restore)
        except Exception, e:
            # set reschedulable
            restore.reschedulable = _is_task_reschedulable(restore, e)
            update_restore(restore, properties="reschedulable")
            raise

    ###########################################################################
    def _do_run_restore(self, restore):
        """
            Does the actual restore. Must be overridden by subclasses
        """

    ###########################################################################
    def cleanup_restore(self, restore):

        # delete the temp dir
        workspace = restore.workspace
        logger.info("Cleanup: deleting workspace dir %s" % workspace)
        update_restore(restore, event_name="CLEANUP",
                       message="Running cleanup")

        try:

            if os.path.exists(workspace):
                shutil.rmtree(workspace)
            else:
                logger.error("workspace dir %s does not exist!" % workspace)
        except Exception, e:
            logger.error("Cleanup error for task '%s': %s" % (restore.id, e))

    ###########################################################################
    def _compute_restore_destination_stats(self, restore):
        logger.info("Computing destination stats for restore '%s'" %
                    restore.id)
        dest_connector = build_mongo_connector(restore.destination.uri)
        restore.destination_stats = dest_connector.get_stats()
        update_restore(restore, properties=["destinationStats"])

    ###########################################################################
    # Helpers
    ###########################################################################
    def _validate_max_data_size(self, backup):
        if (self.max_data_size and
            backup.source_stats and
            backup.source_stats.get("dataSize") and
            backup.source_stats.get("dataSize") > self.max_data_size):

            data_size = backup.source_stats.get("dataSize")
            database_name = backup.source.database_name
            raise SourceDataSizeExceedsLimits(data_size=data_size,
                                              max_size=self.max_data_size,
                                              database_name=database_name)


    ###########################################################################
    def _fsynclock(self, backup, mongo_connector):
        if isinstance(mongo_connector, MongoServer):
            msg = "Running fsynclock on '%s'" % mongo_connector
            update_backup(backup, event_name="FSYNCLOCK", message=msg)
            mongo_connector.fsynclock()
        else:
            raise ConfigurationError("Invalid fsynclock attempt. '%s' has to"
                                     " be a MongoServer" % mongo_connector)

    ###########################################################################
    def _fsyncunlock(self, backup, mongo_connector):
        if isinstance(mongo_connector, MongoServer):
            msg = "Running fsyncunlock on '%s'" % mongo_connector
            update_backup(backup, event_name="FSYNCUNLOCK", message=msg)
            mongo_connector.fsyncunlock()
        else:
            raise ConfigurationError("Invalid fsyncunlock attempt. '%s' has to"
                                     " be a MongoServer" % mongo_connector)

    ###########################################################################
    def _suspend_io(self, backup, mongo_connector, cloud_block_storage):

        if isinstance(mongo_connector, MongoServer):
            if not mongo_connector.is_local():
                err = ("Cannot suspend io for '%s' because is not local to"
                       " this box" % mongo_connector)
                raise ConfigurationError(err)

            msg = "Suspend IO for '%s' using fsfreeze" % mongo_connector
            update_backup(backup, event_name="SUSPEND_IO", message=msg)
            cloud_block_storage.suspend_io()
        else:
            raise ConfigurationError("Invalid suspend io attempt. '%s' has to"
                                     " be a MongoServer" % mongo_connector)

    ###########################################################################
    def _resume_io(self, backup, mongo_connector, cloud_block_storage):

        if isinstance(mongo_connector, MongoServer):
            if not mongo_connector.is_local():
                err = ("Cannot resume io for '%s' because is not local to "
                       "this box" % mongo_connector)
                raise ConfigurationError(err)

            msg = "Resume io for '%s' using fsfreeze" % mongo_connector
            update_backup(backup, event_name="RESUME_IO", message=msg)
            cloud_block_storage.resume_io()
        else:
            raise ConfigurationError("Invalid resume io attempt. '%s' has "
                                     "to be a MongoServer" % mongo_connector)

    ###########################################################################
    def _set_backup_name_and_desc(self, backup):
        if not backup.name:
            backup.name = self.get_backup_name(backup)

        if not backup.description:
            backup.description = self.get_backup_description(backup)

        update_backup(backup, properties=["name", "description"])

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

        return doc

###############################################################################
# Dump Strategy Classes
###############################################################################
class DumpStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        BackupStrategy.__init__(self)
        self._force_table_scan = None

    ###########################################################################
    @property
    def force_table_scan(self):
        return self._force_table_scan

    @force_table_scan.setter
    def force_table_scan(self, val):
        self._force_table_scan = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = BackupStrategy.to_document(self, display_only=display_only)
        doc.update({
            "_type": "DumpStrategy"
        })

        if self.force_table_scan is not None:
            doc["forceTableScan"] = self.force_table_scan

        return doc

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=30,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def do_backup_mongo_connector(self, backup, mongo_connector):
        """
            Override
        """
        source = backup.source

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

                # TODO maybe change the name of the uploaded failed dump log file
                self._upload_dump_log_file(backup)
                self._tar_and_upload_failed_dump(backup)
                raise


        # tar the dump
        if not backup.is_event_logged(EVENT_END_ARCHIVE):
            self._archive_dump(backup)
            # delete dump dir to save space since its not needed any more
            self._delete_dump_dir(backup)

        # upload back file to the target
        if not backup.is_event_logged(EVENT_END_UPLOAD):
            self._upload_dump(backup)

    ###########################################################################
    def _archive_dump(self, backup):
        dump_dir = self._get_backup_dump_dir(backup)
        tar_filename = _tar_file_name(backup)
        logger.info("Taring dump %s to %s" % (dump_dir, tar_filename))
        update_backup(backup,
                      event_name=EVENT_START_ARCHIVE,
                      message="Taring dump")

        self._execute_tar_command(dump_dir, tar_filename)

        update_backup(backup,
                      event_name=EVENT_END_ARCHIVE,
                      message="Taring completed")

    ###########################################################################
    def _delete_dump_dir(self, backup):

        # delete the temp dir
        dump_dir = self._get_backup_dump_dir(backup)
        logger.info("Deleting dump dir %s" % dump_dir)
        update_backup(backup, event_name="DELETE_DUMP_DIR",
                      message="Deleting dump dir")

        try:

            if os.path.exists(dump_dir):
                shutil.rmtree(dump_dir)
            else:
                logger.error("dump dir %s does not exist!" % dump_dir)
        except Exception, e:
            logger.error("Error while deleting dump dir for backup '%s': %s" %
                         (backup.id, e))

    ###########################################################################
    def _upload_dump(self, backup):
        tar_file_path = self._get_tar_file_path(backup)
        logger.info("Uploading %s to target" % tar_file_path)

        update_backup(backup,
                      event_name=EVENT_START_UPLOAD,
                      message="Upload tar to target")
        upload_dest_path = _upload_file_dest(backup)

        all_targets = [backup.target]

        if backup.secondary_targets:
            all_targets.extend(backup.secondary_targets)

        # Set Content-Type to x-compressed to avoid it being set by the
        # container especially that s3 sets .tgz to application/x-compressed
        # which causes browsers to download files as .tar
        metadata = {
            "Content-Type": "application/x-compressed"
        }

        # Upload to all targets simultaneously
        target_uploaders = multi_target_upload_file(all_targets,
                                                    tar_file_path,
                                                    destination_path=
                                                    upload_dest_path,
                                                    overwrite_existing=True,
                                                    metadata=metadata)

        # check for errors
        errored_uploaders = filter(lambda uploader: uploader.error is not None,
                                 target_uploaders)

        if errored_uploaders:
            raise errored_uploaders[0].error

        # set the target reference
        target_reference = target_uploaders[0].target_reference

        # keep old target reference if it exists to delete it because it would
        # be the failed file reference
        failed_reference = backup.target_reference
        backup.target_reference = target_reference

        # set the secondary target references
        if backup.secondary_targets:
            secondary_target_references = \
                map(lambda uploader: uploader.target_reference,
                    target_uploaders[1:])

            backup.secondary_target_references = secondary_target_references

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
        log_file_path = self._get_dump_log_path(backup)
        logger.info("Uploading log file for %s to target" % backup.id)

        update_backup(backup, event_name="START_UPLOAD_LOG_FILE",
                      message="Upload log file to target")
        log_dest_path = _upload_log_file_dest(backup)
        log_target_reference = backup.target.put_file(log_file_path,
                                                      destination_path=
                                                        log_dest_path,
                                                      overwrite_existing=True)

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

        dump_dir = self._get_backup_dump_dir(backup)
        failed_tar_filename = _failed_tar_file_name(backup)
        failed_tar_file_path = self._get_failed_tar_file_path(backup)
        failed_dest = _failed_upload_file_dest(backup)
        # tar up
        self._execute_tar_command(dump_dir, failed_tar_filename)
        update_backup(backup,
                      event_name="ERROR_HANDLING_END_TAR",
                      message="Finished taring failed dump")

        # delete bad dump dir to save space
        self._delete_dump_dir(backup)
        # upload
        logger.info("Uploading tar for failed backup '%s' ..." % backup.id)
        update_backup(backup,
                      event_name="ERROR_HANDLING_START_UPLOAD",
                      message="Uploading failed dump tar")

        # upload failed tar file and allow overwriting existing
        target_reference = backup.target.put_file(failed_tar_file_path,
                                                  destination_path=failed_dest,
                                                  overwrite_existing=True)
        backup.target_reference = target_reference

        update_backup(backup, properties="targetReference",
                      event_name="ERROR_HANDLING_END_UPLOAD",
                      message="Finished uploading failed tar")

    ###########################################################################
    def dump_backup(self, backup, mongo_connector, database_name=None):
        """
            Wraps the actual dump command with fsynclock/unlock if needed
        """
        fsync_unlocked = False
        try:
            # run fsync lock if needed

            if self.use_fsynclock:
                self._fsynclock(backup, mongo_connector)

            # backup the mongo connector
            self._do_dump_backup(backup, mongo_connector, database_name=
                                                           database_name)

            # unlock as needed
            if self.use_fsynclock:
                self._fsyncunlock(backup, mongo_connector)
                fsync_unlocked = True

        finally:
            # unlock as needed
            if self.use_fsynclock and not fsync_unlocked:
                self._fsyncunlock(backup, mongo_connector)

    ###########################################################################
    def _do_dump_backup(self, backup, mongo_connector, database_name=None):

        update_backup(backup, event_name=EVENT_START_EXTRACT,
                      message="Dumping backup")

        # dump the the server
        uri = mongo_connector.uri
        uri_wrapper = mongo_uri_tools.parse_mongo_uri(uri)
        if database_name and not uri_wrapper.database:
            if not uri.endswith("/"):
                uri += "/"
            uri += database_name

        mongoctl_exe = which("mongoctl")
        if not mongoctl_exe:
            raise MBSError("mongoctl exe not found in PATH")

        dump_cmd = [mongoctl_exe,
                    "--noninteractive"] # always run with noninteractive

        mongoctl_config_root = get_mbs().mongoctl_config_root
        if mongoctl_config_root:
            dump_cmd.extend([
                "--config-root",
                mongoctl_config_root]
            )

        # DUMP command
        dest = self._get_backup_dump_dir(backup)
        dump_cmd.extend(["dump", uri, "-o", dest])

        # Add --journal for config server backup
        if (isinstance(mongo_connector, MongoServer) and
                mongo_connector.is_config_server()):
            dump_cmd.append("--journal")

        # if its a server level backup then add forceTableScan and oplog
        uri_wrapper = mongo_uri_tools.parse_mongo_uri(uri)
        if not uri_wrapper.database:
            # add forceTableScan if specified
            if self.force_table_scan:
                dump_cmd.append("--forceTableScan")
            if mongo_connector.is_replica_member():
                dump_cmd.append("--oplog")

        # if mongo version is >= 2.4 and we are using admin creds then pass
        # --authenticationDatabase
        mongo_version = mongo_connector.get_mongo_version()
        if (mongo_version >= MongoNormalizedVersion("2.4.0") and
            isinstance(mongo_connector, (MongoServer, MongoCluster))):
            dump_cmd.extend([
                "--authenticationDatabase",
                "admin"
            ])

        # include users in dump if its a database dump and
        # mongo version is >= 2.6.0
        if (mongo_version >= MongoNormalizedVersion("2.6.0") and
                    database_name != None):
            dump_cmd.append("--dumpDbUsersAndRoles")

        dump_cmd_display= dump_cmd[:]
        # mask mongo uri
        dump_cmd_display[dump_cmd_display.index("dump") + 1] = \
            uri_wrapper.masked_uri
        logger.info("Running dump command: %s" % " ".join(dump_cmd_display))

        ensure_dir(dest)
        dump_log_path = self._get_dump_log_path(backup)
        def on_dump_output(line):
            if "ERROR:" in line:
                msg = "Caught a dump error: %s" % line
                update_backup(backup, event_type=EVENT_TYPE_WARNING,
                    event_name="DUMP_ERROR", message=msg)


        # execute dump command
        log_filter_func = get_mbs().dump_line_filter_function
        returncode = execute_command_wrapper(dump_cmd,
                                             output_path=dump_log_path,
                                             on_output=on_dump_output,
                                             output_line_filter=log_filter_func
                                            )

        # read the last dump log line
        last_line_tail_cmd = [which('tail'), '-1', dump_log_path]
        last_dump_line = execute_command(last_line_tail_cmd)

        # raise an error if return code is not 0
        if returncode:
            self._raise_dump_error(dump_cmd_display, returncode,
                                   last_dump_line)

        else:
            update_backup(backup, event_name=EVENT_END_EXTRACT,
                          message="Dump completed")


    ###########################################################################
    def _raise_dump_error(self, dump_command, returncode, last_dump_line):
        if returncode == 245:
            error_type = BadCollectionNameError
        elif "10334" in last_dump_line:
            error_type = InvalidBSONObjSizeError
        elif "13338" in last_dump_line:
            error_type = CappedCursorOverrunError
        elif "13280" in last_dump_line:
            error_type = InvalidDBNameError
        elif "10320" in last_dump_line:
            error_type = BadTypeError
        elif "Cannot connect" in last_dump_line:
            error_type = MongoctlConnectionError
        elif "cursor didn't exist on server" in last_dump_line:
            error_type = CursorDoesNotExistError
        elif "16465" in last_dump_line:
            error_type = ExhaustReceiveError
        elif ("SocketException" in last_dump_line or
              "socket error" in last_dump_line or
              "transport error" in last_dump_line):
            error_type = DumpConnectivityError
        elif "DBClientCursor" in last_dump_line and "failed" in last_dump_line:
            error_type = DBClientCursorFailError
        else:
            error_type = DumpError

        raise error_type(dump_command, returncode, last_dump_line)

    ###########################################################################
    def _execute_tar_command(self, path, filename):

        tar_exe = which("tar")
        working_dir = os.path.dirname(path)
        target_dirname = os.path.basename(path)

        tar_cmd = [tar_exe, "-cvzf", filename, target_dirname]
        cmd_display = " ".join(tar_cmd)

        try:
            logger.info("Running tar command: %s" % cmd_display)
            execute_command(tar_cmd, cwd=working_dir)

        except CalledProcessError, e:
            if "No space left on device" in e.output:
                error_type = NoSpaceLeftError
            else:
                error_type = ArchiveError

            raise error_type(cmd_display, e.returncode, e.output, e)

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
    def _get_backup_dump_dir(self, backup):
        return os.path.join(backup.workspace, _backup_dump_dir_name(backup))

    ###########################################################################
    def _get_dump_log_path(self, backup):
        dump_dir = self._get_backup_dump_dir(backup)
        return os.path.join(backup.workspace, dump_dir, _log_file_name(backup))

    ###########################################################################
    def _get_tar_file_path(self, backup):
        return os.path.join(backup.workspace, _tar_file_name(backup))

    ###########################################################################
    def _get_failed_tar_file_path(self, backup):
        return os.path.join(backup.workspace, _failed_tar_file_name(backup))

    ###########################################################################
    def _get_restore_log_path(self, restore):
        return os.path.join(restore.workspace, _restore_log_file_name(restore))

    ###########################################################################
    # Restore implementation
    ###########################################################################
    def _do_run_restore(self, restore):

        logger.info("Running dump restore '%s'" % restore.id)

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
                self._upload_restore_log_file(restore)
        except RestoreError, e:
            self._upload_restore_log_file(restore)
            raise


    ###########################################################################
    def _download_source_backup(self, restore):
        backup = restore.source_backup
        file_reference = backup.target_reference

        logger.info("Downloading restore '%s' dump tar file '%s'" %
                    (restore.id, file_reference.file_name))

        update_restore(restore, event_name="START_DOWNLOAD_BACKUP",
                       message="Download source backup file...")

        backup.target.get_file(file_reference, restore.workspace)

        update_restore(restore, event_name="END_DOWNLOAD_BACKUP",
                       message="Source backup file download complete!")


    ###########################################################################
    def _extract_source_backup(self, restore):
        working_dir = restore.workspace
        file_reference = restore.source_backup.target_reference
        logger.info("Extracting tar file '%s'" % file_reference.file_name)

        update_restore(restore, event_name="START_EXTRACT_BACKUP",
                       message="Extract backup file...")

        tarx_cmd = [
            which("tar"),
            "-xf",
            file_reference.file_name
        ]

        logger.info("Running tar extract command: %s" % tarx_cmd)
        try:
            execute_command(tarx_cmd, cwd=working_dir)
        except CalledProcessError, cpe:
            logger.error("Failed to execute extract command: %s" % tarx_cmd)
            raise ExtractError(tarx_cmd, cpe.returncode, cpe.output, cause=cpe)


        update_restore(restore, event_name="END_EXTRACT_BACKUP",
                       message="Extract backup file completed!")

    ###########################################################################
    def _restore_dump(self, restore):
        working_dir = restore.workspace
        file_reference = restore.source_backup.target_reference

        logger.info("Extracting tar file '%s'" % file_reference.file_name)

        update_restore(restore, event_name="START_RESTORE_DUMP",
                       message="Starting mongorestore...")

        # run mongoctl restore
        logger.info("Restoring using mongoctl restore")
        restore_source_path = file_reference.file_name[: -4]
        restore_source_path = os.path.join(working_dir, restore_source_path)
        # IMPORTANT delete dump log file so the restore command would not break
        if restore.source_backup.log_target_reference:
            log_file = restore.source_backup.log_target_reference.file_name
            dump_log_path = os.path.join(restore_source_path, log_file)
            os.remove(dump_log_path)

        dest_uri = restore.destination.uri

        # connect to the destination
        mongo_connector = build_mongo_connector(dest_uri)

        dest_uri_wrapper = mongo_uri_tools.parse_mongo_uri(dest_uri)

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
            restore_source_path = os.path.join(restore_source_path,
                                               source_database_name)
            if not dest_uri_wrapper.database:
                if not dest_uri.endswith("/"):
                    dest_uri += "/"
                dest_uri += source_database_name

        restore_cmd = [
            which("mongoctl"),
            "restore",
            dest_uri,
            restore_source_path
        ]

        # append  --oplogReplay for cluster backups/restore
        if (not source_database_name and
            "repl" in restore.source_backup.source_stats):
            restore_cmd.append("--oplogReplay")

        # if mongo version is >= 2.4 and we are using admin creds then pass
        # --authenticationDatabase

        mongo_version = mongo_connector.get_mongo_version()
        if (mongo_version >= MongoNormalizedVersion("2.4.0") and
                isinstance(mongo_connector, (MongoServer, MongoCluster))) :
            restore_cmd.extend([
                "--authenticationDatabase",
                "admin"
            ])

        # include users in restore if its a database restore and
        # mongo version is >= 2.6.0
        if (mongo_version >= MongoNormalizedVersion("2.6.0") and
                    source_database_name != None):
            restore_cmd.append("--restoreDbUsersAndRoles")

        restore_cmd_display = restore_cmd[:]

        restore_cmd_display[restore_cmd_display.index("restore") + 1] =\
            dest_uri_wrapper.masked_uri


        logger.info("Running mongoctl restore command: %s" %
                    " ".join(restore_cmd_display))
        # execute dump command
        restore_log_path = self._get_restore_log_path(restore)
        returncode = execute_command_wrapper(restore_cmd,
            output_path=restore_log_path,
            cwd=working_dir
        )

        # read the last dump log line
        last_line_tail_cmd = [which('tail'), '-1', restore_log_path]
        last_log_line = execute_command(last_line_tail_cmd)

        if returncode:
            raise RestoreError(restore_cmd_display, returncode, last_log_line)

        update_restore(restore, event_name="END_RESTORE_DUMP",
                       message="Restoring dump completed!")

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

        address = mongo_connector.address
        cbs = self.get_backup_cbs(backup, mongo_connector)

        # validate
        if not cbs:
            msg = ("Cannot run a block storage snapshot backup for backup '%s'"
                   ".Backup source does not have a cloudBlockStorage "
                   "configured for address '%s'" % (backup.id, address))
            raise ConfigurationError(msg)

        update_backup(backup, event_name="START_BLOCK_STORAGE_SNAPSHOT",
                      message="Starting snapshot backup...")

        # kickoff the snapshot if it was not kicked off before
        if not backup.is_event_logged("END_KICKOFF_SNAPSHOT"):
            self._kickoff_snapshot(backup, mongo_connector, cbs)

        # wait until snapshot is completed or error
        wait_status = [CBS_STATUS_COMPLETED, CBS_STATUS_ERROR]
        self._wait_for_snapshot_status(backup, cbs, wait_status)

        snapshot_ref = backup.target_reference

        if snapshot_ref.status == CBS_STATUS_COMPLETED:
            logger.info("Successfully completed backup '%s' snapshot" %
                        backup.id)
            msg = "Snapshot completed successfully"
            update_backup(backup, event_name="END_BLOCK_STORAGE_SNAPSHOT",
                          message=msg)
        else:
            raise BlockStorageSnapshotError("Snapshot error")

    ###########################################################################
    def _kickoff_snapshot(self, backup, mongo_connector, cbs):
        """
        Creates the snapshot and waits until it is kicked off (state pending)
        :param backup:
        :param cbs:
        :return:
        """

        use_fysnclock = (self.backup_mode == BackupMode.ONLINE and
                         mongo_connector.is_online() and
                         self.is_use_fsynclock())
        use_suspend_io = self.is_use_suspend_io() and use_fysnclock
        fsync_unlocked = False

        resumed_io = False
        try:
            update_backup(backup, event_name="START_KICKOFF_SNAPSHOT",
                          message="Kicking off snapshot")

            # run fsync lock
            if use_fysnclock:
                self._fsynclock(backup, mongo_connector)
            else:
                msg = ("Snapshot Backup '%s' will be taken WITHOUT "
                       "locking database and IO!" % backup.id)
                logger.warning(msg)
                update_backup(backup, event_type=EVENT_TYPE_WARNING,
                              event_name="NOT_LOCKED",
                              message=msg)

            # suspend io
            if use_suspend_io:
                self._suspend_io(backup, mongo_connector, cbs)

            # create the snapshot
            self._create_snapshot(backup, cbs)

            # wait until snapshot is pending or completed or error
            wait_status = [CBS_STATUS_PENDING, CBS_STATUS_COMPLETED,
                           CBS_STATUS_ERROR]
            self._wait_for_snapshot_status(backup, cbs, wait_status)

            # resume io/unlock

            if use_suspend_io:
                self._resume_io(backup, mongo_connector, cbs)
                resumed_io = True

            if use_fysnclock:
                self._fsyncunlock(backup, mongo_connector)
                fsync_unlocked = True

            update_backup(backup, event_name="END_KICKOFF_SNAPSHOT",
                          message="Snapshot kicked off successfully!")

        finally:
            try:
                # resume io/unlock as needed
                if use_suspend_io and not resumed_io:
                    self._resume_io(backup, mongo_connector, cbs)
            finally:
                if use_fysnclock and not fsync_unlocked:
                    self._fsyncunlock(backup, mongo_connector)

    ###########################################################################
    def _create_snapshot(self, backup, cbs):

        logger.info("Initiating block storage snapshot for backup '%s'" %
                    backup.id)

        update_backup(backup, event_name="START_CREATE_SNAPSHOT",
                      message="Creating snapshot")

        if isinstance(cbs, CompositeBlockStorage):
            name_template = self.constituent_name_scheme or backup.name
            desc_template = (self.constituent_description_scheme or
                             backup.description)
            snapshot_ref = cbs.create_snapshot(name_template, desc_template)
        else:
            name = self.get_backup_name(backup)
            desc = self.get_backup_description(backup)
            snapshot_ref = cbs.create_snapshot(name, desc)

        backup.target_reference = snapshot_ref

        msg = "Snapshot created successfully"

        update_backup(backup, properties="targetReference",
                      event_name="END_CREATE_SNAPSHOT",
                      message=msg)

    ###########################################################################
    def _wait_for_snapshot_status(self, backup, cbs, wait_status):
        logger.info("Waiting for backup '%s' snapshot status to be in %s" %
                    (backup.id, wait_status))
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
                logger.info("Old: \n%s\nNew:\n%s" % (snapshot_ref,
                                                     new_snapshot_ref))
                snapshot_ref = new_snapshot_ref
                backup.target_reference = snapshot_ref
                update_backup(backup, properties="targetReference")

            time.sleep(5)


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
        if (backup.target_reference and
            isinstance(backup.target_reference,
                       CloudBlockStorageSnapshotReference)):
            return backup.target_reference.cloud_block_storage
        else:
            return self.get_backup_source_cbs(backup.source, mongo_connector)

    ###########################################################################
    def get_backup_source_cbs(self, source, mongo_connector):
        address = mongo_connector.address
        return source.get_block_storage_by_address(address)

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

        if self.use_fsynclock is not None:
            strategy.use_fsynclock = self.use_fsynclock

        strategy.backup_name_scheme = \
            (strategy.backup_name_scheme or
             self.backup_name_scheme)

        strategy.backup_description_scheme = \
            (strategy.backup_description_scheme or
             self.backup_description_scheme)

    ###########################################################################
    def _do_run_restore(self, restore):
        if restore.source_backup.is_event_logged(EVENT_END_EXTRACT):
            return self.dump_strategy._do_run_restore(restore)
        else:
            return self.cloud_block_storage_strategy._do_run_restore(restore)

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

    ###########################################################################
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
    def _snapshot_backup(self, backup, mongo_connector):
        """
            Override!
        """
        # call super method
        suber = super(EbsVolumeStorageStrategy, self)
        suber._snapshot_backup(backup, mongo_connector)

        self._apply_snapshot_shares(backup)

    ###########################################################################
    def _apply_snapshot_shares(self, backup):
        logger.info("Checking if snapshot backup '%s' is configured to be "
                    "shared" % backup.id)
        is_sharing = self.share_users or self.share_groups

        if is_sharing:
            share_snapshot_backup(backup, user_ids=self.share_users,
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
def share_snapshot_backup(backup, user_ids=None, groups=None):
    msg = ("Sharing snapshot backup '%s' with users:%s, groups:%s " %
           (backup.id, user_ids, groups))
    logger.info(msg)

    target_ref = backup.target_reference
    if isinstance(target_ref, LVMSnapshotReference):
        logger.info("Sharing All constituent snapshots for LVM backup"
                    " '%s'..." % backup.id)
        for cs in target_ref.constituent_snapshots:
            cs.share_snapshot(user_ids=user_ids, groups=groups)
    elif isinstance(target_ref, EbsSnapshotReference):
        target_ref.share_snapshot(user_ids=user_ids,
                                  groups=groups)
    else:
        raise ValueError("Cannot share a non EBS/LVM Backup '%s'" % backup.id)

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
    return map(lambda log_event: log_event.name, backup.get_warnings())


