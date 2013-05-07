__author__ = 'abdul'


import os
import time

import shutil
import mbs_logging
import mongo_uri_tools

from mbs import get_mbs

from base import MBSObject
from persistence import update_backup
from mongo_utils import (MongoCluster, MongoDatabase, MongoServer,
                         MongoNormalizedVersion)

from subprocess import CalledProcessError
from errors import *
from utils import (which, ensure_dir, execute_command, execute_command_wrapper)
from target import CBS_STATUS_COMPLETED, CBS_STATUS_ERROR


from backup import EVENT_TYPE_WARNING
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

# Member preference values
PREF_PRIMARY_ONLY = "PRIMARY_ONLY"
PREF_SECONDARY_ONLY = "SECONDARY_ONLY"
PREF_BEST = "BEST"

###############################################################################
# LOGGER
###############################################################################

logger = mbs_logging.logger

###############################################################################
def _is_backup_reschedulable( backup, exception):
        return (backup.try_count < MAX_NO_RETRIES and
                is_exception_retriable(exception))

###############################################################################
# BackupStrategy Classes
###############################################################################
class BackupStrategy(MBSObject):

    ###########################################################################
    def __init__(self):
        self._member_preference = PREF_BEST
        self._max_data_size = None
        self._backup_name_scheme = None
        self._backup_description_scheme = None

    ###########################################################################
    @property
    def member_preference(self):
        return self._member_preference

    @member_preference.setter
    def member_preference(self, val):
        self._member_preference = val

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
    def run_backup(self, backup):
        try:
            self._do_run_backup(backup)
        except Exception, e:
            # set reschedulable
            backup.reschedulable = _is_backup_reschedulable(backup, e)
            update_backup(backup, properties="reschedulable")
            raise

    ###########################################################################
    def _do_run_backup(self, backup):

        mongo_connector = self.get_backup_mongo_connector(backup)
        self.backup_mongo_connector(backup, mongo_connector)

    ###########################################################################
    def get_backup_mongo_connector(self, backup):
        uri_wrapper = mongo_uri_tools.parse_mongo_uri(backup.source.uri)
        if uri_wrapper.is_cluster_uri() and not uri_wrapper.database:
            return self._select_backup_cluster_member(backup)
        elif not uri_wrapper.database:
            return MongoServer(backup.source.uri)
        else:
            return MongoDatabase(backup.source.uri)

    ###########################################################################
    def _select_backup_cluster_member(self, backup):
        source = backup.source
        mongo_cluster = MongoCluster(source.uri)

        # compute max lag
        if backup.plan:
            max_lag_seconds = int(backup.plan.schedule.frequency_in_seconds / 2)
        else:
            # One Off backup : no max lag!
            max_lag_seconds = 0

        # find a server to dump from

        primary_member = mongo_cluster.primary_member
        selected_member = None
        # dump from best secondary if configured and found
        if (self.member_preference in [PREF_BEST, PREF_SECONDARY_ONLY] and
            backup.try_count < MAX_NO_RETRIES):

            best_secondary = mongo_cluster.get_best_secondary(max_lag_seconds=
                                                               max_lag_seconds)

            if best_secondary:
                selected_member = best_secondary
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
            self.member_preference in [PREF_BEST, PREF_PRIMARY_ONLY]):
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

        source = backup.source
        dbname = source.database_name
        # record stats
        backup.source_stats = mongo_connector.get_stats(only_for_db=dbname)

        # save source stats
        update_backup(backup, properties="sourceStats",
            event_name="COMPUTED_SOURCE_STATS",
            message="Computed source stats")

        # set backup name and description
        self._set_backup_name_and_desc(backup)

        # validate max data size if set
        self._validate_max_data_size(backup)

        # backup the mongo connector
        self.do_backup_mongo_connector(backup, mongo_connector)

    ###########################################################################
    def do_backup_mongo_connector(self, backup, mongo_connector):
        """
            Does the actual work. Has to be overridden by subclasses
        """


    ###########################################################################
    def cleanup_backup(self, backup):

        # delete the temp dir
        workspace = backup.workspace
        logger.info("Cleanup: deleting workspace dir %s" % workspace)
        update_backup(backup,
            event_name="CLEANUP",
            message="Running cleanup")

        try:

            if os.path.exists(workspace):
                shutil.rmtree(workspace)
            else:
                logger.error("workspace dir %s does not exist!" % workspace)
        except Exception, e:
            logger.error("Cleanup error for backup '%s': %s" % (backup.id, e))

    ###########################################################################
    def restore_backup(self, backup):
        """
            Does the actual work. Has to be overridden by subclasses
        """

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

        return naming_scheme.generate_name(backup)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "memberPreference": self.member_preference
        }

        if self.max_data_size:
            doc["maxDataSize"] = self.max_data_size

        if self.backup_name_scheme:
            doc["backupNameScheme"] = \
                self.backup_name_scheme.to_document(display_only=False)

        if self.backup_description_scheme:
            doc["backupDescriptionScheme"] =\
                self.backup_description_scheme.to_document(display_only=False)

        return doc

###############################################################################
# Dump Strategy Classes
###############################################################################
class DumpStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        BackupStrategy.__init__(self)
        self._ensure_localhost = False

    ###########################################################################
    @property
    def ensure_localhost(self):
        return self._ensure_localhost

    @ensure_localhost.setter
    def ensure_localhost(self, val):
        self._ensure_localhost = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc =  BackupStrategy.to_document(self, display_only=display_only)
        doc.update({
            "_type": "DumpStrategy",
            "ensureLocalhost": self.ensure_localhost
        })

        return doc

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=30,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def do_backup_mongo_connector(self, backup, mongo_connector):
        """
            Override
        """
        # ensure local host if specified
        if self.ensure_localhost and not mongo_connector.is_local():
            details = ("Source host for dump source '%s' is not localhost and"
                       " strategy.ensureLocalHost is set to true" %
                       mongo_connector)
            raise DumpNotOnLocalhost(msg="Error while attempting to dump",
                                     details=details)
        source = backup.source

        # ensure backup workspace
        ensure_dir(backup.workspace)

        # run mongoctl dump
        if not backup.is_event_logged(EVENT_END_EXTRACT):
            try:
                self._do_dump_backup(backup, mongo_connector,
                                     database_name=source.database_name)
            except DumpError, e:
                # still tar and upload failed dumps
                logger.error("Dumping backup '%s' failed. Will still tar"
                             "up and upload to keep dump logs" % backup.id)
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
    def _upload_dump(self, backup):
        tar_file_path = self._get_tar_file_path(backup)
        logger.info("Uploading %s to target" % tar_file_path)

        update_backup(backup,
                      event_name=EVENT_START_UPLOAD,
                      message="Upload tar to target")
        upload_dest_path = _upload_file_dest(backup)
        target_reference = backup.target.put_file(tar_file_path,
            destination_path=upload_dest_path)

        # keep old target reference if it exists to delete it because it would
        # be the failed file reference
        failed_reference = backup.target_reference
        backup.target_reference = target_reference

        update_backup(backup, properties="targetReference",
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

        dump_cmd = [which("mongoctl"),
                    "--noninteractive"] # always run with noninteractive

        mongoctl_config_root = get_mbs().mongoctl_config_root
        if mongoctl_config_root:
            dump_cmd.extend([
                "--config-root",
                mongoctl_config_root]
            )

        dest = self._get_backup_dump_dir(backup)
        dump_cmd.extend(["dump", uri, "-o", dest])

        # if its a server level backup then add forceTableScan and oplog
        uri_wrapper = mongo_uri_tools.parse_mongo_uri(uri)
        if not uri_wrapper.database:
            dump_cmd.append("--forceTableScan")
            if mongo_connector.is_replica_member():
                dump_cmd.append("--oplog")

        # if mongo version is >= 2.4 and we are using admin creds then pass
        # --authenticationDatabase
        mongo_version = mongo_connector.get_mongo_version()
        if (mongo_version >= MongoNormalizedVersion("2.4.0") and
            isinstance(mongo_connector, (MongoServer, MongoCluster))) :
            dump_cmd.extend([
                "--authenticationDatabase",
                "admin"
            ])

        dump_cmd_display= dump_cmd[:]
        # if the source uri is a mongo uri then mask it
        dump_cmd_display[dump_cmd_display.index("dump") + 1] = \
            uri_wrapper.masked_uri
        logger.info("Running dump command: %s" % " ".join(dump_cmd_display))

        ensure_dir(dest)
        dump_log_path = os.path.join(dest, 'dump.log')
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
    def _get_backup_dump_dir(self, backup):
        return os.path.join(backup.workspace, _backup_dump_dir_name(backup))

    ###########################################################################
    def _get_tar_file_path(self, backup):
        return os.path.join(backup.workspace,
                            _tar_file_name(backup))

    ###########################################################################
    def _get_failed_tar_file_path(self, backup):
        return os.path.join(backup.workspace,
            _failed_tar_file_name(backup))

###############################################################################
# Helpers
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
def _failed_upload_file_dest(backup):
    dest =  "%s.tgz" % backup.name
    # append FAILED as a prefix for the file name  + handle the case where
    # backup name is a path (as appose to just a file name)
    parts = dest.rpartition("/")
    return "%s%sFAILED_%s" % (parts[0], parts[1], parts[2])

###############################################################################
# CloudBlockStorageStrategy
###############################################################################
class CloudBlockStorageStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        BackupStrategy.__init__(self)

    ###########################################################################
    def do_backup_mongo_connector(self, backup, mongo_connector):

        source = backup.source
        address = mongo_connector.address
        cloud_block_storage = source.get_block_storage_by_address(address)
        # validate
        if not cloud_block_storage:
            msg = ("Cannot run a block storage snapshot backup for backup '%s'"
                   ".Backup source does not have a cloudBlockStorage "
                   "configured for address '%s'" % (backup.id, address))
            raise ConfigurationError(msg)

        logger.info("Kicking off block storage snapshot for backup '%s'" %
                    backup.id)

        update_backup(backup, event_name="START_BLOCK_STORAGE_SNAPSHOT",
                      message="Kicking off snapshot")


        snapshot_ref = cloud_block_storage.create_snapshot(backup.name,
                                                           backup.description)
        backup.target_reference = snapshot_ref

        msg = "Snapshot created successfully"

        update_backup(backup, properties="targetReference",
                      event_name="PENDING_BLOCK_STORAGE_SNAPSHOT", message=msg)

        # wait until snapshot is completed and keep target ref up to date
        while snapshot_ref.status not in [CBS_STATUS_ERROR,
                                          CBS_STATUS_COMPLETED]:
            logger.debug("Checking updates for backup '%s' snapshot '%s' " %
                        (backup.id, snapshot_ref.snapshot_id))
            new_snapshot_ref = cloud_block_storage.check_snapshot_updates(snapshot_ref)
            if new_snapshot_ref:
                logger.info("Detected updates for backup '%s' snapshot '%s' " %
                            (backup.id, snapshot_ref.snapshot_id))
                logger.info("Old: \n%s\nNew:\n%s" % (snapshot_ref,
                                                     new_snapshot_ref))
                snapshot_ref = new_snapshot_ref
                backup.target_reference = snapshot_ref
                update_backup(backup, properties="targetReference")

            else:
                time.sleep(5)


        if snapshot_ref.status == CBS_STATUS_COMPLETED:
            logger.info("Successfully completed backup '%s' snapshot '%s' " %
                        (backup.id, snapshot_ref.snapshot_id))
            msg = "Snapshot completed successfully"
            update_backup(backup, properties="targetReference",
                event_name="END_BLOCK_STORAGE_SNAPSHOT", message=msg)
        else:
            raise BlockStorageSnapshotError("Snapshot error")

    ###########################################################################
    def to_document(self, display_only=False):
        doc =  BackupStrategy.to_document(self, display_only=display_only)
        doc.update({
            "_type": "CloudBlockStorageStrategy"
        })

        return doc

###############################################################################
# Hybrid Strategy Class
###############################################################################
DUMP_MAX_DATA_SIZE = 50  * 1024 * 1024 * 1024

class HybridStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        BackupStrategy.__init__(self)
        self._dump_strategy = DumpStrategy()
        self._cloud_block_storage_strategy = CloudBlockStorageStrategy()
        self._predicate = DataSizePredicate()

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
    def cloud_block_storage_strategy(self):
        return self._cloud_block_storage_strategy

    @cloud_block_storage_strategy.setter
    def cloud_block_storage_strategy(self, val):
        self._cloud_block_storage_strategy = val

    ###########################################################################
    def _do_run_backup(self, backup):
        mongo_connector = self.get_backup_mongo_connector(backup)

        selected_strategy = self.predicate.get_best_strategy(self, backup,
                                                             mongo_connector)
        selected_strategy.backup_mongo_connector(backup, mongo_connector)

    ###########################################################################
    def _set_backup_name_and_desc(self, backup):
        """
         Do nothing so that the selected strategy will take care of that
         instead
        """

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

            "predicate": self.predicate.to_document(display_only=display_only)
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
            block_storage = backup.source.get_block_storage_by_address(address)
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
