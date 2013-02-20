__author__ = 'abdul'


import os
import shutil
import mbs_logging
import mongo_uri_tools

from base import MBSObject
from persistence import update_backup
from mongo_utils import MongoCluster, MongoDatabase, MongoServer
from subprocess import CalledProcessError
from errors import *
from utils import (which, ensure_dir, execute_command, call_command, wait_for)



from backup import EVENT_TYPE_WARNING
from robustify.robustify import robustify
from target import EbsSnapshotReference

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
# LOGGER
###############################################################################

logger = mbs_logging.logger

###############################################################################
# Error Handling Helpers
###############################################################################

def _is_exception_retriable(exception):
    return isinstance(exception, RetriableError)

def _raise_if_not_retriable(exception):
    if _is_exception_retriable(exception):
        logger.warn("Caught a retriable exception: %s" % exception)
    else:
        logger.debug("Re-raising a a NON-retriable exception: %s" % exception)
        raise


###############################################################################
def _is_backup_reschedulable( backup, exception):
        return (backup.try_count < MAX_NO_RETRIES and
                _is_exception_retriable(exception))

###############################################################################
def _raise_on_failure():
    raise

###############################################################################
# BackupStrategy Classes
###############################################################################
class BackupStrategy(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

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
        """
            Does the actual work. Has to be overridden by subclasses
        """
        pass

    ###########################################################################
    def cleanup_backup(self, backup):
        pass

    ###########################################################################
    def restore_backup(self, backup):
        pass

###############################################################################
# Dump Strategy Classes
###############################################################################
class DumpStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        self._primary_ok = True
        self._use_best_secondary = True
        self._ensure_localhost = False

    ###########################################################################
    @property
    def primary_ok(self):
        return self._primary_ok

    @primary_ok.setter
    def primary_ok(self, val):
        self._primary_ok = val

    ###########################################################################
    @property
    def use_best_secondary(self):
        return self._use_best_secondary

    @use_best_secondary.setter
    def use_best_secondary(self, val):
        self._use_best_secondary = val

    ###########################################################################
    @property
    def ensure_localhost(self):
        return self._ensure_localhost

    @ensure_localhost.setter
    def ensure_localhost(self, val):
        self._ensure_localhost = val

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "DumpStrategy",
            "primaryOk": self.primary_ok,
            "useBestSecondary": self.use_best_secondary,
            "ensureLocalhost": self.ensure_localhost
        }

    ###########################################################################
    @robustify(max_attempts=3, retry_interval=30,
               do_on_exception=_raise_if_not_retriable,
               do_on_failure=_raise_on_failure)
    def _do_run_backup(self, backup):
        # ensure backup workspace
        ensure_dir(backup.workspace)

        # run mongoctl dump
        if not backup.is_event_logged(EVENT_END_EXTRACT):
            try:
                self._dump_source(backup)
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
    def _dump_source(self, backup):

        logger.info("Dumping source %s " % backup.source)
        update_backup(backup,
                      event_name=EVENT_START_EXTRACT,
                      message="Dumping source")

        # log warning if dumping from a cluster's primary or from a too
        # stale member

        uri_wrapper = mongo_uri_tools.parse_mongo_uri(backup.source.uri)
        if uri_wrapper.is_cluster_uri() and not uri_wrapper.database:
            self._dump_cluster_source(backup)
        elif not uri_wrapper.database:
            self._dump_server_source(backup)
        else:
            self._dump_database_source(backup)


        update_backup(backup,
                      event_name=EVENT_END_EXTRACT,
                      message="Dump completed")

    ###########################################################################
    def _dump_cluster_source(self, backup):

        source = backup.source
        mongo_cluster = MongoCluster(source.uri)

        # compute minimum required lag
        if backup.plan:
            min_lag_seconds = int(backup.plan.schedule.frequency_in_seconds / 2)
        else:
            # One Off backup : no min lag and primary OK!
            min_lag_seconds = 0

        # find a server to dump from

        primary_member = mongo_cluster.primary_member
        selected_member = None
        # dump from best secondary if configured and found
        if (self.use_best_secondary and
            backup.try_count < MAX_NO_RETRIES):

            best_secondary = mongo_cluster.get_best_secondary(min_lag_seconds=
                                                               min_lag_seconds)

            if best_secondary:
                selected_member = best_secondary
                # log warning if secondary is too stale
                if best_secondary.is_too_stale():
                    logger.warning("Backup '%s' will be extracted from a "
                                   "too stale member!" % backup.id)

                    msg = ("Warning! The dump will be extracted from a too "
                           "stale member")
                    update_backup(backup,
                                  event_type=EVENT_TYPE_WARNING,
                                  event_name="USING_TOO_STALE_WARNING",
                                  message=msg)


        if not selected_member and self.primary_ok:
            # otherwise dump from primary if primary ok or if this is the
            # last try. log warning because we are dumping from a primary
            selected_member = primary_member
            logger.warning("Backup '%s' will be extracted from the "
                           "primary!" % backup.id)

            msg = "Warning! The dump will be extracted from the  primary"
            update_backup(backup,
                          event_type=EVENT_TYPE_WARNING,
                          event_name="USING_PRIMARY_WARNING",
                          message=msg)

        if not selected_member:
            # error out
            raise NoEligibleMembersFound(source.uri)

        self._do_dump_server(backup, selected_member)

    ###########################################################################
    def _do_dump_server(self, backup, mongo_server):

        # ensure local host if specified
        if self.ensure_localhost and not mongo_server.is_local():
            details = ("Source host for dump source '%s' is not localhost and"
                       " strategy.ensureLocalHost is set to true" %
                       mongo_server)
            raise DumpNotOnLocalhost(msg="Error while attempting to dump",
                                     details=details)
        source = backup.source
        # record stats
        if source.database_name:
            backup.source_stats = mongo_server.get_stats(only_for_db=
                                                          source.database_name)
        else:
            backup.source_stats = mongo_server.get_stats()

        # save source stats
        update_backup(backup, properties="sourceStats",
                      event_name="COMPUTED_SOURCE_STATS",
                      message="Computed source stats")

        # dump the the server
        uri = mongo_server.uri
        if source.database_name:
            if not uri.endswith("/"):
                uri += "/"
            uri += source.database_name

        self._do_dump_backup(backup, uri)

    ###########################################################################
    def _dump_server_source(self, backup):
        mongo_server = MongoServer(backup.source.uri)
        self._do_dump_server(backup, mongo_server)

    ###########################################################################
    def _dump_database_source(self, backup):
        mongo_database = MongoDatabase(backup.source.uri)
        # record stats
        backup.source_stats = mongo_database.get_stats()

        # save source stats
        update_backup(backup, properties="sourceStats",
                      event_name="COMPUTED_SOURCE_STATS",
                      message="Computed source stats")

        self._do_dump_backup(backup, backup.source.uri)

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

        backup.target_reference = target_reference

        update_backup(backup, properties="targetReference",
                      event_name=EVENT_END_UPLOAD,
                      message="Upload completed!")

    ###########################################################################
    def _tar_and_upload_failed_dump(self, backup):
        logger.info("Taring up failed backup '%s' ..." % backup.id)
        update_backup(backup,
                      event_name="ERROR_HANDLING_START_TAR",
                      message="Taring bad dump")

        dump_dir = self._get_backup_dump_dir(backup)
        tar_filename = _tar_file_name(backup)
        tar_file_path = self._get_tar_file_path(backup)

        # tar up
        self._execute_tar_command(dump_dir, tar_filename)
        update_backup(backup,
                      event_name="ERROR_HANDLING_END_TAR",
                      message="Finished taring bad dump")

        # upload
        logger.info("Uploading tar for failed backup '%s' ..." % backup.id)
        update_backup(backup,
                      event_name="ERROR_HANDLING_START_UPLOAD",
                      message="Uploading bad tar")

        target_reference = backup.target.put_file(tar_file_path)
        backup.target_reference = target_reference

        update_backup(backup, properties="targetReference",
                      event_name="ERROR_HANDLING_END_UPLOAD",
                      message="Finished uploading bad tar")

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
    def _do_dump_backup(self, backup, uri):

        dest = self._get_backup_dump_dir(backup)
        dump_cmd = ["/usr/local/bin/mongoctl",
                    "--noninteractive", # always run with noninteractive
                    "dump", uri,
                    "-o", dest]

        # if its a server level backup then add forceTableScan and oplog
        uri_wrapper = mongo_uri_tools.parse_mongo_uri(uri)
        if not uri_wrapper.database:
            dump_cmd.extend([
                "--oplog",
                "--forceTableScan"]
            )

        dump_cmd_display= dump_cmd[:]
        # if the source uri is a mongo uri then mask it

        dump_cmd_display[3] = uri_wrapper.masked_uri
        logger.info("Running dump command: %s" % " ".join(dump_cmd_display))

        try:
            # execute dump command and redirect stdout and stderr to log file
            ensure_dir(dest)
            ## IMPORTANT: TEMPORARILY EXCLUDE DUMP FILES FROM DEST FOLDER
            # dump_log_path = os.path.join(dest, 'dump.log')
            # TODO: uncomment the line above and remove the line bellow
            dump_log_path = os.path.join(os.path.dirname(dest), 'dump.log')
            dump_log_file = open(dump_log_path, 'w')
            call_command(dump_cmd, stdout=dump_log_file,
                stderr=dump_log_file)

        except CalledProcessError, e:
            # read the last dump log line
            last_line_tail_cmd = [which('tail'), '-1', dump_log_path]
            last_dump_line = execute_command(last_line_tail_cmd)
            # select proper error type to raise
            if e.returncode == 245:
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
            else:
                error_type = DumpError

            raise error_type(dump_cmd_display, e.returncode, last_dump_line, e)

    ###########################################################################
    def _execute_tar_command(self, path, filename):

        try:
            tar_exe = which("tar")
            working_dir = os.path.dirname(path)
            target_dirname = os.path.basename(path)

            tar_cmd = [tar_exe, "-cvzf", filename, target_dirname]
            cmd_display = " ".join(tar_cmd)

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


###############################################################################
# Helpers
###############################################################################

def _tar_file_name(backup):
    return "%s.tgz" % _backup_dump_dir_name(backup)

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
# CloudBlockStorageStrategy
###############################################################################
class CloudBlockStorageStrategy(BackupStrategy):

    ###########################################################################
    def _do_run_backup(self, backup):

        cloud_block_storage = backup.source.cloud_block_storage
        # validate
        if not cloud_block_storage:
            msg = ("Cannot run a block storage snapshot backup for backup '%s'"
                   ".Backup source does not have a cloudBlockStorage "
                   "configured" % backup.id)
            raise ConfigurationError(msg)

        logger.info("Kicking off block storage snapshot for backup '%s'" %
                    backup.id)

        update_backup(backup, event_name="START_BLOCK_STORAGE_SNAPSHOT",
                      message="Kicking off snapshot")

        snapshot_desc = _backup_dump_dir_name(backup)
        target_reference = cloud_block_storage.create_snapshot(snapshot_desc)
        backup.target_reference = target_reference

        msg = "Snapshot created successfully"

        update_backup(backup, properties="targetReference",
                      event_name="END_BLOCK_STORAGE_SNAPSHOT", message=msg)


    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "CloudBlockStorageStrategy"
        }

###############################################################################
# Hybrid Strategy Class
###############################################################################
DUMP_MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024

class HybridStrategy(BackupStrategy):

    ###########################################################################
    def __init__(self):
        self._dump_strategy = DumpStrategy()
        self._cloud_block_storage_strategy = CloudBlockStorageStrategy()
        self._predicate = FileSizePredicate()

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
        selected_strategy = self.predicate.get_best_strategy(self, backup)
        selected_strategy.run_backup(backup)

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "HybridStrategy",
            "dumpStrategy":
                self.dump_strategy.to_document(display_only=display_only),

            "cloudBlockStorageStrategy":
                self.cloud_block_storage_strategy.to_document(display_only=
                                                               display_only),

            "predicate": self.predicate.to_document(display_only=display_only)
        }

###############################################################################
# HybridStrategyPredicate
###############################################################################
class HybridStrategyPredicate(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def get_best_strategy(self, hybrid_strategy, backup):
        """
            Returns the best strategy to be used for running the specified
            backup
            Must be overridden by subclasses
        """
        pass

###############################################################################
# FileSizePredicate
###############################################################################
class FileSizePredicate(HybridStrategyPredicate):

    ###########################################################################
    def __init__(self):
        self._dump_max_file_size = 10

    ###########################################################################
    def get_best_strategy(self, hybrid_strategy, backup):
        """
            Returns the best strategy to be used for running the specified
            backup
            Must be overridden by subclasses
        """
        file_size = self._get_backup_source_file_size(backup)
        logger.info("Selecting best strategy for backup '%s', fileSize=%s, "
                    "dump max file size=%s" %
                    (backup.id, file_size, self.dump_max_file_size))

        if file_size < self.dump_max_file_size:
            logger.info("Selected dump strategy since fileSize %s is less"
                        " than dump max size %s" %
                        (file_size, self.dump_max_file_size))
            return hybrid_strategy.dump_strategy
        else:
            logger.info("Selected cloud block storage strategy since "
                        "fileSize %s is more than dump max size %s" %
                        (file_size, self.dump_max_file_size))
            return hybrid_strategy.cloud_block_storage_strategy


    ###########################################################################
    @property
    def dump_max_file_size(self):
        return self._dump_max_file_size

    @dump_max_file_size.setter
    def dump_max_file_size(self, val):
        self._dump_max_file_size = val

    ###########################################################################
    def _get_backup_source_file_size(self, backup):
        source = backup.source
        uri_wrapper = mongo_uri_tools.parse_mongo_uri(source.uri)
        if uri_wrapper.is_cluster_uri() and not uri_wrapper.database:
            cluster = MongoCluster(source.uri)
            if source.database_name:
                stats = cluster.primary_member.get_stats(only_for_db=
                                                          source.database_name)
            else:
                stats = cluster.primary_member.get_stats()
        elif not uri_wrapper.database:
            server = MongoServer(source.uri)
            if source.database_name:
                stats = server.primary_member.get_stats(only_for_db=
                                                         source.database_name)
            else:
                stats = server.primary_member.get_stats()
        else:
            db = MongoDatabase(source.uri)
            stats = db.get_stats()

        return stats["fileSize"]

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "FileSizePredicate",
            "dumpMaxFileSize":self.dump_max_file_size
        }