__author__ = 'abdul'


import traceback
import os

import time
import mbs_logging
import shutil

from threading import Thread
from subprocess import CalledProcessError

from errors import MBSException

from utils import (which, ensure_dir, execute_command,
                   wait_for, resolve_path, timedelta_total_seconds, date_now)

from plan import STRATEGY_DUMP, STRATEGY_EBS_SNAPSHOT

from backup import (STATE_SCHEDULED, STATE_IN_PROGRESS, STATE_FAILED,
                    STATE_SUCCEEDED)

from target import EbsSnapshotReference

###############################################################################
# CONSTANTS
###############################################################################

DEFAULT_BACKUP_TEMP_DIR_ROOT = "~/backup_temp"

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
########################                       ################################
######################## Backup Engine/Workers ################################
########################                       ################################
###############################################################################

###############################################################################
# BackupEngine
###############################################################################
class BackupEngine(Thread):

    ###########################################################################
    def __init__(self, engine_id, backup_collection, max_workers=10,
                       sleep_time=10,
                       temp_dir=None,
                       notification_handler=None):
        Thread.__init__(self)
        self._engine_id = engine_id
        self._backup_collection = backup_collection
        self._sleep_time = sleep_time
        self._worker_count = 0
        self._max_workers = max_workers
        self._temp_dir = resolve_path(temp_dir or DEFAULT_BACKUP_TEMP_DIR_ROOT)
        self._notification_handler = notification_handler

    ###########################################################################
    @property
    def engine_id(self):
        return self._engine_id

    ###########################################################################
    @property
    def backup_collection(self):
        return self._backup_collection

    ###########################################################################
    @property
    def max_workers(self):
        return self._max_workers

    ###########################################################################
    @property
    def temp_dir(self):
        return self._temp_dir

    ###########################################################################
    def run(self):
        self.info("Starting up... ")
        self.info("TEMP DIR is '%s'" % self.temp_dir)
        ensure_dir(self._temp_dir)
        self._recover()

        while True:
            self.info("Reading next scheduled backup...")
            backup = self.read_next_backup()
            self._start_backup(backup)

    ###########################################################################
    def _start_backup(self, backup):
        # if max workers are reached then sleep
        while self._worker_count >= self.max_workers:
            time.sleep(self._sleep_time)

        self.info("Received  backup %s" % backup)
        worker_id = self.next_worker_id()
        self.info("Starting backup %s, BackupWorker %s" %
                  (backup._id, worker_id))
        BackupWorker(worker_id, backup, self).start()

    ###########################################################################
    def next_worker_id(self):
        self._worker_count+= 1
        return self._worker_count

    ###########################################################################
    def worker_fail(self, worker, exception, trace=None):
        log_msg = "Failure! Cause %s\nTrace:\n%s" % (exception,trace)
        self.worker_finished(worker, STATE_FAILED, message=log_msg)

        backup = worker.backup
        if self._notification_handler:
            subject = "Backup '%s' failed" % backup.id
            message = ("Backup '%s' failed.\n%s\n\nCause: \n%s\nStack Trace:"
                       "\n%s" % (backup.id, backup, exception, trace))

            self._notification_handler.send_notification(subject, message)


    ###########################################################################
    def worker_success(self, worker):
        self.worker_finished(worker, STATE_SUCCEEDED,
                             message="Backup completed successfully!")

    ###########################################################################
    def worker_finished(self, worker, state, message):
        self._worker_count -= 1
        self.update_backup_state(worker.backup, state, message=message)

    ###########################################################################
    def update_backup_state(self, backup, state, message=None):
        backup.change_state(state, message)
        self._backup_collection.save_document(backup.to_document())

    ###########################################################################
    def log_backup_event(self, backup, name, message=None):
        backup.log_event(name, message=message)
        self._backup_collection.save_document(backup.to_document())


    ###########################################################################
    def _recover(self):
        """
        Does necessary recovery work on crashes.
         1- Recover recoverable in-progress backups associated with this engine
         otherwise Reschedule if possible
         2- Wipes the temp dir
        """
        self.info("Running recovery..")

        # 1- Recover recoverable backups
        q = {
            "state": STATE_IN_PROGRESS,
            "engineId": self.engine_id
        }

        total_recovered = 0
        total_failed = 0
        for backup in self._backup_collection.find(q):
            if self._is_backup_recoverable(backup):
                self._recover_backup(backup)
                total_recovered += 1
            else:
                # fail backup
                self.info("Recovery: Failing backup %s" % backup._id)
                self.update_backup_state(backup, STATE_FAILED)
                total_failed += 1


        total_crashed = total_recovered + total_failed

        self.info("Recovery complete! Total Crashed backups:%s,"
                  " Total scheduled for recovery=%s, Total failed=%s" %
                  (total_crashed, total_recovered, total_failed))

    ###########################################################################
    def _recover_backup(self, backup):
        self.info("Recovery: Resuming backup '%s'" % backup.id)
        self.log_backup_event(backup,
                              name="RECOVERY",
                              message="Resuming backup")

        self._start_backup(backup)

    ###########################################################################
    def _is_backup_recoverable(self, backup):
        return backup.is_event_logged([EVENT_END_EXTRACT,
                                       EVENT_END_ARCHIVE,
                                       EVENT_END_UPLOAD])

    ###########################################################################
    def read_next_backup(self):
        q = {"state" : STATE_SCHEDULED}
        u = {"$set" : { "state" : STATE_IN_PROGRESS,
                        "engineId": self.engine_id}}

        c = self._backup_collection
        backup = None
        while backup is None:
            time.sleep(self._sleep_time)
            backup = c.find_and_modify(query=q, update=u)

        backup.engine_id = self.engine_id

        return backup

    ###########################################################################
    def info(self, msg):
        logger.info("<BackupEngine-%s>: %s" % (self.engine_id, msg))

    ###########################################################################
    def error(self, msg):
        logger.error("<BackupEngine-%s>: %s" % (self.engine_id, msg))

###############################################################################
# BackupWorker
###############################################################################

class BackupWorker(Thread):

    ###########################################################################
    def __init__(self, id, backup, engine):
        Thread.__init__(self)
        self._id = id
        self._backup = backup
        self._engine = engine

    ###########################################################################
    @property
    def backup(self):
        return self._backup

    ###########################################################################
    @property
    def engine(self):
        return self._engine

    ###########################################################################
    def run(self):
        backup = self.backup
        self.info("Running %s backup %s" % (backup.strategy, backup._id))
        backup.change_state(STATE_IN_PROGRESS)

        try:

            if backup.strategy == STRATEGY_DUMP:
                self._run_dump_backup(backup)
            elif backup.strategy == STRATEGY_EBS_SNAPSHOT:
                self._run_ebs_snapshot_backup(backup)
            else:
                raise BackupEngineException("Unsupported backup strategy '%s'" %
                                            backup.strategy)

            # success!
            self.engine.worker_success(self)
            self.info("Backup '%s' completed successfully" % backup.id)
        except Exception, e:
            # fail
            self.error("Backup failed. Cause %s" % e)
            trace = traceback.format_exc()
            self.error(trace)

            self.engine.worker_fail(self, exception=e, trace=trace)

    ###########################################################################
    # DUMP Strategy
    ###########################################################################
    def _run_dump_backup(self, backup):
        temp_dir = None
        tar_file_path = None

        try:

            temp_dir = self._ensure_temp_dir(backup)
            tar_filename = "%s.tgz" % self.backup_dir_name(backup)
            tar_file_path = os.path.join(self.engine.temp_dir, tar_filename)

            # run mongoctl dump
            if not backup.is_event_logged(EVENT_END_EXTRACT):
                self.info("Dumping source %s " % backup.source)
                self.engine.log_backup_event(backup,
                                             name=EVENT_START_EXTRACT,
                                             message="Dumping source")

                # record source stats
                backup.source_stats = backup.source.get_current_stats()

                self._dump_source(backup.source, temp_dir)
                self.engine.log_backup_event(backup,
                                             name=EVENT_END_EXTRACT,
                                             message="Dump completed")

            # tar the dump
            if not backup.is_event_logged(EVENT_END_ARCHIVE):
                self.info("Taring dump %s to %s" % (temp_dir, tar_filename))
                self.engine.log_backup_event(backup,
                                             name=EVENT_START_ARCHIVE,
                                             message="Taring dump")

                self._tar_dir(temp_dir, tar_filename)

                self.engine.log_backup_event(backup,
                                             name=EVENT_END_ARCHIVE,
                                             message="Taring completed")

            # upload back file to the target
            if not backup.is_event_logged(EVENT_END_UPLOAD):
                self.info("Uploading %s to target" % tar_file_path)
                self.engine.log_backup_event(backup,
                                             name=EVENT_START_UPLOAD,
                                             message="Upload tar to target")

                # set the target reference and it will be saved by the next
                # log event call
                target_reference = backup.target.put_file(tar_file_path)
                backup.target_reference = target_reference

                self.engine.log_backup_event(backup,
                                             name=EVENT_END_UPLOAD,
                                             message="Upload completed!")

            # calculate backup rate
            self._calculate_backup_rate(backup)
        finally:
            # cleanup
            # delete the temp dir
            self.info("Cleanup: deleting temp dir %s" % temp_dir)
            self.engine.log_backup_event(backup,
                                         name="CLEANUP",
                                         message="Running cleanup")

            if temp_dir:
                shutil.rmtree(temp_dir)
                # delete the gzip
                self.info("Cleanup: tar file %s" % tar_file_path)
                if tar_file_path and os.path.exists(tar_file_path):
                    os.remove(tar_file_path)
                else:
                    self.error("tar file %s does not exists!" %
                              tar_file_path)
            else:
                self.error("temp dir %s does not exist!" % temp_dir)

    ###########################################################################
    def _dump_source(self, source, dest):
        dump_cmd = ["/usr/local/bin/mongoctl",
                    "dump", source.source_address,
                    "-o",dest]

        # if its a server level backup then add forceTableScan and oplog
        if not source.database_name:
            dump_cmd.extend([
                "--oplog",
                "--forceTableScan"]
            )


        self.info("Running dump command: %s" % " ".join(dump_cmd))

        try:
            execute_command(dump_cmd)
        except CalledProcessError, e:
            msg = ("Failed to dump. Dump command '%s' returned a non-zero exit"
                   " status %s. Command output:\n%s" %
                   (dump_cmd, e.returncode, e.output))
            raise BackupEngineException(msg)

    ###########################################################################
    def _tar_dir(self, path, filename):

        try:
            tar_exe = which("tar")
            working_dir = os.path.dirname(path)
            target_dirname = os.path.basename(path)

            tar_cmd = [tar_exe, "-cvzf", filename, target_dirname]
            cmd_display = " ".join(tar_cmd)

            self.info("Running tar command: %s" % cmd_display)
            execute_command(tar_cmd, cwd=working_dir)

        except CalledProcessError, e:
            msg = ("Failed to tar. Tar command '%s' returned a non-zero exit"
                   " status %s. Command output:\n%s" %
                   (cmd_display, e.returncode, e.output))
            raise BackupEngineException(msg)

    ###########################################################################
    def _ensure_temp_dir(self, backup):
        temp_dir = os.path.join(self.engine.temp_dir,
            self.backup_dir_name(backup))
        if not os.path.exists(temp_dir):
            self.info("Creating temp dir '%s' for backup %s" %
                      (temp_dir, backup._id))
            os.makedirs(temp_dir)

        return temp_dir

    ###########################################################################
    def backup_dir_name(self, backup):
        return "%s" % backup.id


    def _calculate_backup_rate(self, backup):
        duration = timedelta_total_seconds(date_now() - backup.start_date)
        if backup.source_stats and backup.source_stats.get("fileSizeInGB"):
            size = backup.source_stats["fileSizeInGB"]
            rate = size/duration
            backup.backup_rate = rate

    ###########################################################################
    # EBS Snapshot Strategy
    ###########################################################################
    def _run_ebs_snapshot_backup(self, backup):


        ebs_volume_source = backup.source
        self.info("Getting backup source volume '%s'" %
                  ebs_volume_source.volume_id)

        self.engine.log_backup_event(backup,
                                     name="GET_EBS_VOLUME",
                                     message="Getting volume '%s'" %
                                             ebs_volume_source.volume_id)

        volume = ebs_volume_source.get_volume()

        self.info("Kicking off ebs snapshot for backup '%s' volumeId '%s'" %
                  (backup.id, ebs_volume_source.volume_id))

        self.engine.log_backup_event(backup,
                                     name="START_EBS_SNAPSHOT",
                                     message="Kicking off snapshot")

        snapshot_desc = self.backup_dir_name(backup)
        if not volume.create_snapshot(snapshot_desc):
            raise BackupEngineException("Failed to create snapshot from backup"
                                        " source :\n%s" % ebs_volume_source)
        else:
            # get the snapshot id and put it as a target reference
            snapshot = ebs_volume_source.get_snapshot_by_desc(snapshot_desc)
            self.info("Snapshot kicked off successfully. Snapshot id '%s'." %
                      snapshot.id)

            msg = ("Snapshot kicked off successfully. Snapshot id '%s'. "
                   "Waiting for snapshot to complete..." % snapshot.id)

            self.engine.log_backup_event(backup,
                                         name="EBS_START_SUCCESS",
                                         message=msg)

            def log_func():
                self.info("Waiting for snapshot '%s' status to be completed" %
                          snapshot.id)

            def is_completed():
                snapshot = ebs_volume_source.get_snapshot_by_desc(snapshot_desc)
                return snapshot.status == 'completed'

            # log a waiting msg
            log_func() # :)
             # wait until complete
            wait_for(is_completed, timeout=300, log_func=log_func )

            if is_completed():
                self.info("Snapshot '%s' completed successfully!." %
                          snapshot.id)
                backup.target_reference = EbsSnapshotReference(snapshot.id)
            else:
                raise BackupEngineException("Snapshot Timeout error")



    ###########################################################################
    def info(self, msg):
        self._engine.info("Worker-%s: %s" % (self._id, msg))

    ###########################################################################
    def error(self, msg):
        self._engine.error("Worker-%s: %s" % (self._id, msg))

###############################################################################
# BackupEngineException
###############################################################################
class BackupEngineException(MBSException):

    ###########################################################################
    def __init__(self, message, cause=None):
        MBSException.__init__(self, message, cause=cause)