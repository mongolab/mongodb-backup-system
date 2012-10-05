__author__ = 'abdul'

import sys
import traceback
import os
import logging
import time
import mbs_logging
import shutil

from threading import Thread
from utils import which, ensure_dir, execute_command, timestamp_to_dir_str

from backup import (Backup, STATE_SCHEDULED, STATE_IN_PROGRESS, STATE_FAILED,
                    STATE_SUCCEEDED, STATE_CANCELED)
###############################################################################
# CONSTANTS
###############################################################################

BACKUP_TEMP_DIR_ROOT = os.path.expanduser("~/backup_temp")

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
    def __init__(self, engine_id, backup_collection):
        Thread.__init__(self)
        self._engine_id = engine_id
        self._backup_collection = backup_collection
        self._sleep_time = 1
        self._worker_count = 0

        ensure_dir(BACKUP_TEMP_DIR_ROOT)

    ###########################################################################
    @property
    def engine_id(self):
        return self._engine_id

    ###########################################################################
    @property
    def backup_collection(self):
        return self._backup_collection

    ###########################################################################
    def run(self):
        self.info("Starting up... ")
        while True:
            self.info("Reading next scheduled backup...")
            backup = self.read_next_backup()
            self.info("Received  backup %s" % backup)
            worker_id = self.next_worker_id()
            self.info("Starting BackupWorker %s for backup %s" %
                      (worker_id, backup._id))
            BackupWorker(worker_id, backup, self).start()

    ###########################################################################
    def next_worker_id(self):
        self._worker_count+= 1
        return self._worker_count

    ###########################################################################
    def backup_success(self, backup):
        self.update_backup_state(backup, STATE_SUCCEEDED)

    ###########################################################################
    def backup_fail(self, backup):
        self.update_backup_state(backup, STATE_FAILED)

    ###########################################################################
    def update_backup_state(self, backup, state):
        backup.change_state(state)
        self._backup_collection.save_document(backup.to_document())

    ###########################################################################
    def log_backup_event(self, backup, message):
        backup.log_event(message)
        self._backup_collection.save_document(backup.to_document())

    ###########################################################################
    def read_next_backup(self):
        q = {"state" : STATE_SCHEDULED}
        u = {"$set" : { "state" : STATE_IN_PROGRESS,
                        "engine_id": self._engine_id}}

        c = self._backup_collection
        backup = None
        while backup is None:
            time.sleep(self._sleep_time)
            backup = c.find_and_modify(query=q, update=u)

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
        temp_dir = None
        tar_file_path = None

        try:
            backup = self.backup
            self.info("Running backup %s" % backup._id)
            backup.change_state(STATE_IN_PROGRESS)

            self.engine.log_backup_event(backup, "Creating temp dir")

            temp_dir = self._create_temp_dir(backup)

            # run mongoctl dump
            self.info("Dumping source %s " % backup.source)
            self.engine.log_backup_event(backup, "Dumping source")

            self._dump_source(backup.source, temp_dir)
            self.engine.log_backup_event(backup, "Dump completed")

            # tar the dump
            tar_filename = "%s.tgz" % self.backup_dir_name(backup)
            self.info("Taring dump %s to %s" % (temp_dir, tar_filename))
            self.engine.log_backup_event(backup, "Taring dump")

            tar_file_path = self._tar_dir(temp_dir, tar_filename)
            self.engine.log_backup_event(backup, "Taring completed")

            # upload back file to the target
            self.info("Uploading %s to target" % tar_file_path)
            self.engine.log_backup_event(backup, "Upload tar to target")

            backup.target.put_file(tar_file_path)
            self.engine.log_backup_event(backup, "Upload completed")

            # success!
            self.engine.backup_success(backup)

        except Exception, e:
            # fail
            traceback.print_exc()
            self.error("Backup failed. Cause %s" % e)
            self.engine.log_backup_event(backup,"Backup failure. Cause %s" % e)

            self.engine.backup_fail(backup)
        finally:
            # cleanup
            # delete the temp dir
            self.info("Cleanup: deleting temp dir %s" % temp_dir)
            self.engine.log_backup_event(backup, "Running cleanup")

            if temp_dir:
                shutil.rmtree(temp_dir)
                # delete the gzip
                self.info("Cleanup: tar file %s" % tar_file_path)
                if tar_file_path and os.path.exists(tar_file_path):
                    os.remove(tar_file_path)
                else:
                    self.info("tar file %s does not exists!!!" %
                              tar_file_path)
            else:
                self.info("temp dir %s does not exists!!!" % temp_dir)

    ###########################################################################
    def _dump_source(self, source, dest):
        dump_cmd = ["/usr/local/bin/mongoctl",
                    "dump", source.source_uri,
                    "-o",dest]

        if source.username:
            dump_cmd.extend(["-u", source.username])
        if source.password:
            dump_cmd.extend(["-p", source.password])

        # if the source hosted database ==> then always use
        # TODO --use-best-secondary
        #if type(source) in [HostedDatabaseSource, MongoLabClusterSource]:
         #   dump_cmd.append("--use-best-secondary")

        cmd_display =  dump_cmd[:]
        # mask password
        if source.password:
            cmd_display[cmd_display.index("-p") + 1] =  "****"

        self.info("Running command: %s" % " ".join(cmd_display))

        execute_command(dump_cmd, call=True)

    ###########################################################################
    def _tar_dir(self, path, filename):

        tar_exe = which("tar")
        working_dir = os.path.dirname(path)
        target_dirname = os.path.basename(path)

        tar_cmd = [tar_exe,
                   "-cvzf", filename, target_dirname]
        self.info("Running command: %s" % " ".join(tar_cmd))
        execute_command(tar_cmd, cwd=working_dir, call=True)
        return os.path.join(BACKUP_TEMP_DIR_ROOT, filename)

    ###########################################################################
    def _create_temp_dir(self, backup):
        temp_dir = os.path.join(BACKUP_TEMP_DIR_ROOT,
            self.backup_dir_name(backup))
        if not os.path.exists(temp_dir):
            self.info("Creating temp dir '%s' for backup %s" %
                      (temp_dir, backup._id))
            os.makedirs(temp_dir)

        return temp_dir

    ###########################################################################
    def backup_dir_name(self, backup):
        return "%s_%s_%s" % (backup.plan._id,
                             backup._id,
                             timestamp_to_dir_str(backup.timestamp))


    ###########################################################################
    def info(self, msg):
        self._engine.info("Worker-%s: %s" % (self._id, msg))

    ###########################################################################
    def error(self, msg):
        self._engine.error("Worker-%s: %s" % (self._id, msg))

