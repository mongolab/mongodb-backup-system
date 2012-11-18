__author__ = 'abdul'


import traceback
import os

import time
import mbs_logging
import shutil
import urllib
import mongo_uri_tools

from flask import Flask
from flask.globals import request

from threading import Thread
from subprocess import CalledProcessError

from errors import MBSException

from utils import (which, ensure_dir, execute_command, call_command,
                   wait_for, resolve_path, get_local_host_name)

from date_utils import  timedelta_total_seconds, date_now

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

STATUS_RUNNING = "running"
STATUS_STOPPING = "stopping"
STATUS_STOPPED = "stopped"

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
    def __init__(self, id=None, backup_collection=None, max_workers=10,
                       sleep_time=1,
                       temp_dir=None,
                       notification_handler=None,
                       command_port=8888):
        Thread.__init__(self)
        self._id = id
        self._engine_guid = None
        self._backup_collection = backup_collection
        self._sleep_time = sleep_time
        self._worker_count = 0
        self._max_workers = int(max_workers)
        self._temp_dir = resolve_path(temp_dir or DEFAULT_BACKUP_TEMP_DIR_ROOT)
        self._notification_handler = notification_handler
        self._stopped = False
        self._command_port = command_port
        self._command_server = EngineCommandServer(self)
        self._tags = None

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    ###########################################################################
    @property
    def engine_guid(self):
        if not self._engine_guid:
            self._engine_guid = get_local_host_name() + "-" + self.id
        return self._engine_guid

    ###########################################################################
    @property
    def backup_collection(self):
        return self._backup_collection

    @backup_collection.setter
    def backup_collection(self, val):
        self._backup_collection = val

    ###########################################################################
    @property
    def max_workers(self):
        return self._max_workers

    @max_workers.setter
    def max_workers(self, max_workers):
        self._max_workers = max_workers

    ###########################################################################
    @property
    def temp_dir(self):
        return self._temp_dir

    @temp_dir.setter
    def temp_dir(self, temp_dir):
        self._temp_dir = resolve_path(temp_dir)

    ###########################################################################
    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        tags = tags or {}
        self._tags = self._resolve_tags(tags)

    ###########################################################################
    @property
    def command_port(self):
        return self._command_port

    @command_port.setter
    def command_port(self, command_port):
        self._command_port = command_port

    ###########################################################################
    @property
    def notification_handler(self):
        return self._notification_handler

    @notification_handler.setter
    def notification_handler(self, handler):
        self._notification_handler = handler

    ###########################################################################
    def run(self):
        self.info("Starting up... ")
        self.info("TEMP DIR is '%s'" % self.temp_dir)
        ensure_dir(self._temp_dir)
        # Start the command server
        self._start_command_server()
        self._recover()

        while not self._stopped:
            # wait until we have workers available
            self._wait_for_workers_availability()
            # Now that we have workers available ==> read next backup
            self.info("Reading next scheduled backup...")
            backup = self.read_next_backup()
            if backup:
                self._start_backup(backup)

        self.info("Exited main loop")
        self._pre_shutdown()

    ###########################################################################
    def _start_backup(self, backup):
        self.info("Received  backup %s" % backup)
        worker_id = self.next_worker_id()
        self.info("Starting backup %s, BackupWorker %s" %
                  (backup._id, worker_id))
        BackupWorker(worker_id, backup, self).start()

    ###########################################################################
    def _wait_for_workers_availability(self):
    # if max workers are reached then sleep
        while self._worker_count >= self.max_workers:
            time.sleep(self._sleep_time)

    ###########################################################################
    def next_worker_id(self):
        self._worker_count+= 1
        return self._worker_count

    ###########################################################################
    def worker_fail(self, worker, exception, trace=None):
        log_msg = "Failure! Cause %s\nTrace:\n%s" % (exception,trace)
        self.worker_finished(worker, STATE_FAILED, message=log_msg)

        backup = worker.backup
        if self.notification_handler:
            subject = "Backup failed"
            message = ("Backup '%s' failed.\n%s\n\nCause: \n%s\nStack Trace:"
                       "\n%s" % (backup.id, backup, exception, trace))

            self.notification_handler.send_notification(subject, message)


    ###########################################################################
    def worker_success(self, worker):
        self.worker_finished(worker, STATE_SUCCEEDED,
                             message="Backup completed successfully!")

    ###########################################################################
    def worker_finished(self, worker, state, message):
        # set end date
        worker.backup.end_date = date_now()
        # decrease worker count and update state
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
            "engineGuid": self.engine_guid
        }

        total_recovered = 0
        total_failed = 0
        for backup in self._backup_collection.find(q):
            if self._is_backup_recoverable(backup):
                # wait until we have workers available
                self._wait_for_workers_availability()
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


        q = self._get_backups_query()
        u = {"$set" : { "state" : STATE_IN_PROGRESS,
                        "engineGuid": self.engine_guid}}

        c = self._backup_collection
        backup = None
        while not self._stopped and backup is None:
            time.sleep(self._sleep_time)
            backup = c.find_and_modify(query=q, update=u)

        if backup:
            backup.engine_guid = self.engine_guid
        return backup

    ###########################################################################
    def _get_backups_query(self):
        q = {"state" : STATE_SCHEDULED}

        # add tags if specified
        if self.tags:
            tag_filters = []
            for name,value in self.tags.items():
                tag_prop_path = "tags.%s" % name
                tag_filters.append({tag_prop_path: value})

            q["$or"] = tag_filters
        else:
            q["$or"]= [
                    {"tags" : {"$exists": False}},
                    {"tags" : {}},
                    {"tags" : None}
            ]

        return q

    ###########################################################################
    def _get_tag_bindings(self):
        """
            Returns a dict of binding name/value that will be used for
            resolving tags. Binding names starts with a '$'.
            e.g. "$HOST":"FOO"
        """
        return {
            "$HOST": get_local_host_name()
        }

    ###########################################################################
    def _resolve_tags(self, tags):
        resolved_tags = {}
        for name,value in tags.items():
            resolved_tags[name] = self._resolve_tag_value(value)

        return resolved_tags

    ###########################################################################
    def _resolve_tag_value(self, value):
        for binding_name, binding_value in self._get_tag_bindings().items():
            value = value.replace(binding_name, binding_value)

        return value

    ###########################################################################
    # Engine stopping
    ###########################################################################
    def stop(self):
        """
            Sends a stop request to the engine using the command port
            This should be used by other processes (copies of the engine
            instance) but not the actual running engine process
        """

        url = "http://0.0.0.0:%s/stop" % self.command_port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                print response.read().strip()
            else:
                msg =  ("Error while trying to stop engine '%s' URL %s "
                        "(Response"" code %)" %
                        (self.engine_guid, url, response.getcode()))
                raise BackupEngineException(msg)
        except IOError, e:
            logger.error("Engine is not running")

    ###########################################################################
    def get_status(self):
        """
            Sends a status request to the engine using the command port
            This should be used by other processes (copies of the engine
            instance) but not the actual running engine process
        """
        url = "http://0.0.0.0:%s/status" % self.command_port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                return response.read().strip()
            else:
                msg =  ("Error while trying to get status engine '%s' URL %s "
                        "(Response code %)" % (self.engine_guid, url,
                                               response.getcode()))
                raise BackupEngineException(msg)

        except IOError, ioe:
            return STATUS_STOPPED

    ###########################################################################
    def _do_stop(self):
        """
            Stops the engine gracefully by waiting for all workers to finish
            and not starting any new workers.
            Returns true if it will stop immediately (i.e. no workers running)
        """
        self.info("Stopping engine gracefully. Waiting for %s workers"
                  " to finish" % self._worker_count)

        self._stopped = True
        return self._worker_count == 0

    ###########################################################################
    def _do_get_status(self):
        """
            Gets the status of the engine
        """
        if self._stopped:
            return STATUS_STOPPING
        else:
            return STATUS_RUNNING

    ###########################################################################
    def _pre_shutdown(self):
        self._stop_command_server()

    ###########################################################################
    # Command Server
    ###########################################################################

    def _start_command_server(self):
        self.info("Starting command server at port %s" % self._command_port)

        self._command_server.start()
        self.info("Command Server started successfully!")

    ###########################################################################
    def _stop_command_server(self):
        self._command_server.stop()

    ###########################################################################
    # Logging methods
    ###########################################################################
    def info(self, msg):
        logger.info("<BackupEngine-%s>: %s" % (self.id, msg))

    ###########################################################################
    def error(self, msg):
        logger.error("<BackupEngine-%s>: %s" % (self.id, msg))

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
        # set start date
        backup.start_date = date_now()
        # apply the retention policy
        # TODO Probably should be called somewhere else
        self._apply_retention_policy(backup.plan)

        # set state to be in progress
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
        finally:
            # apply the retention policy
            # TODO Probably should be called somewhere else
            self._apply_retention_policy(backup.plan)

    ###########################################################################
    # DUMP Strategy
    ###########################################################################
    def _run_dump_backup(self, backup):


        try:
            # ensure that the te
            self._ensure_temp_dir(backup)


            # run mongoctl dump
            if not backup.is_event_logged(EVENT_END_EXTRACT):
                try:
                    self._dump_source(backup)
                except BackupEngineException, e:
                    # still tar and upload failed dumps
                    self.error("Dumping backup '%s' failed. Will still tar"
                               "up and upload to keep dump logs" % backup.id)
                    self._tar_and_upload_failed_dump(backup)
                    raise e

            # tar the dump
            if not backup.is_event_logged(EVENT_END_ARCHIVE):
                self._archive_dump(backup)

            # upload back file to the target
            if not backup.is_event_logged(EVENT_END_UPLOAD):
                self._upload_dump(backup)

            # calculate backup rate
                self._calculate_backup_rate(backup)

        finally:
            # cleanup
            self._cleanup_dump(backup)

    ###########################################################################
    def _dump_source(self, backup):
        self.info("Dumping source %s " % backup.source)
        self.engine.log_backup_event(backup,
            name=EVENT_START_EXTRACT,
            message="Dumping source")

        # record source stats
        backup.source_stats = backup.source.get_current_stats()

        # save source stats if present
        if backup.source_stats:
            self.engine.log_backup_event(backup,
                name="COMPUTED_SOURCE_STATS",
                message="Computed source stats")

        temp_dir = self._get_temp_dir(backup)
        self._execute_dump_command(backup.source, temp_dir)


        self.engine.log_backup_event(backup,
                                     name=EVENT_END_EXTRACT,
                                     message="Dump completed")

    ###########################################################################
    def _archive_dump(self, backup):
        temp_dir = self._get_temp_dir(backup)
        tar_filename = _tar_file_name(backup)
        self.info("Taring dump %s to %s" % (temp_dir, tar_filename))
        self.engine.log_backup_event(backup,
            name=EVENT_START_ARCHIVE,
            message="Taring dump")

        self._execute_tar_command(temp_dir, tar_filename)

        self.engine.log_backup_event(backup,
            name=EVENT_END_ARCHIVE,
            message="Taring completed")

    ###########################################################################
    def _upload_dump(self, backup):
        tar_file_path = self._get_tar_file_path(backup)
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

    ###########################################################################
    def _tar_and_upload_failed_dump(self, backup):
        self.info("Taring up failed backup '%s' ..." % backup.id)
        self.engine.log_backup_event(backup,
                                     name="ERROR_HANDLING_START_TAR",
                                     message="Taring bad dump")

        temp_dir = self._get_temp_dir(backup)
        tar_filename = _tar_file_name(backup)
        tar_file_path = self._get_tar_file_path(backup)

        # tar up
        self._execute_tar_command(temp_dir, tar_filename)
        self.engine.log_backup_event(backup,
                                     name="ERROR_HANDLING_END_TAR",
                                     message="Finished taring bad dump")

        # upload
        self.info("Uploading tar for failed backup '%s' ..." % backup.id)
        self.engine.log_backup_event(backup,
                                     name="ERROR_HANDLING_START_UPLOAD",
                                     message="Uploading bad tar")

        backup.target.put_file(tar_file_path)

        self.engine.log_backup_event(backup,
                                     name="ERROR_HANDLING_END_UPLOAD",
                                     message="Finished uploading bad tar")

    ###########################################################################
    def _cleanup_dump(self, backup):
        # delete the temp dir
        temp_dir = self._get_temp_dir(backup)
        tar_file_path = self._get_tar_file_path(backup)
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
    def _execute_dump_command(self, source, dest):
        source_address = source.source_address
        dump_cmd = ["/usr/local/bin/mongoctl",
                    "--noninteractive", # always run with noninteractive
                    "dump", source_address,
                    "-o",dest]

        # if its a server level backup then add forceTableScan and oplog
        if not source.database_name:
            dump_cmd.extend([
                "--oplog",
                "--forceTableScan"]
            )

        dump_cmd_display= dump_cmd[:]
        # if the source uri is a mongo uri then mask it
        if mongo_uri_tools.is_mongo_uri(source_address):
            dump_cmd_display[3] = mongo_uri_tools.mask_mongo_uri(source_address)
        self.info("Running dump command: %s" % " ".join(dump_cmd_display))

        try:
            # execute dump command and redirect stdout and stderr to log file
            ensure_dir(dest)
            dump_log_path = os.path.join(dest, 'dump.log')
            dump_log_file = open(dump_log_path, 'w')
            call_command(dump_cmd, stdout=dump_log_file,
                                   stderr=dump_log_file)

        except CalledProcessError, e:
            msg = ("Failed to dump. Dump command '%s' returned a non-zero exit"
                   " status %s. Check dump logs." %
                   (dump_cmd_display, e.returncode))
            raise BackupEngineException(msg)


    ###########################################################################
    def _execute_tar_command(self, path, filename):

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
        temp_dir = self._get_temp_dir(backup)
        if not os.path.exists(temp_dir):
            self.info("Creating temp dir '%s' for backup %s" %
                      (temp_dir, backup._id))
            os.makedirs(temp_dir)

        return temp_dir

    ###########################################################################
    def _get_temp_dir(self, backup):
        return os.path.join(self.engine.temp_dir,_backup_dir_name(backup))

    ###########################################################################
    def _get_tar_file_path(self, backup):
        return os.path.join(self.engine.temp_dir, _tar_file_name(backup))

    ###########################################################################
    def _calculate_backup_rate(self, backup):
        duration = timedelta_total_seconds(date_now() - backup.start_date)
        if backup.source_stats and backup.source_stats.get("fileSizeInGB"):
            size = backup.source_stats["fileSizeInGB"]
            rate = size/duration
            backup.backup_rate = round(rate, 2)

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
    def _apply_retention_policy(self, plan):
        # apply the plans retention policy
        if plan.retention_policy:
            plan.retention_policy.apply_policy(plan)

    ###########################################################################
    def info(self, msg):
        self._engine.info("Worker-%s: %s" % (self._id, msg))

    ###########################################################################
    def error(self, msg):
        self._engine.error("Worker-%s: %s" % (self._id, msg))


###############################################################################
# Helpers
###############################################################################
def _backup_dir_name(backup):
    return "%s" % backup.id

###############################################################################
def _tar_file_name(backup):
    return "%s.tgz" % _backup_dir_name(backup)

###############################################################################
###############################################################################
# EngineCommandServer
###############################################################################
class EngineCommandServer(Thread):

    ###########################################################################
    def __init__(self, engine):
        Thread.__init__(self)
        self._engine = engine
        self._flask_server = self._build_flask_server()

    ###########################################################################
    def _build_flask_server(self):
        flask_server = Flask(__name__)
        engine = self._engine
        ## build stop method
        @flask_server.route('/stop', methods=['GET'])
        def stop_engine():
            logger.info("Command Server: Received a stop command")
            try:
                if engine._do_stop():
                    return "Engine stopped successfully"
                else:
                    return ("Stop command received. Engine has %s workers "
                            "running and will stop when all workers finish" %
                            engine._worker_count)
            except Exception, e:
                return "Error while trying to stop engine: %s" % e

        ## build status method
        @flask_server.route('/status', methods=['GET'])
        def status():
            logger.info("Command Server: Received a status command")
            try:
                return engine._do_get_status()
            except Exception, e:
                return "Error while trying to get engine status: %s" % e

        ## build stop-command-server method
        @flask_server.route('/stop-command-server', methods=['GET'])
        def stop_command_server():
            logger.info("Stopping command server")
            try:
                shutdown = request.environ.get('werkzeug.server.shutdown')
                if shutdown is None:
                    raise RuntimeError('Not running with the Werkzeug Server')
                shutdown()
                return "success"
            except Exception, e:
                return "Error while trying to get engine status: %s" % e

        return flask_server

    ###########################################################################
    def run(self):
        logger.info("EngineCommandServer: Running flask server ")
        self._flask_server.run(host="0.0.0.0", port=self._engine._command_port,
                               threaded=True)
    ###########################################################################
    def stop(self):

        logger.info("EngineCommandServer: Stopping flask server ")
        port = self._engine._command_port
        url = "http://0.0.0.0:%s/stop-command-server" % port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                logger.info("EngineCommandServer: Flask server stopped "
                            "successfully")
                return response.read().strip()
            else:
                msg =  ("Error while trying to get status engine '%s' URL %s "
                        "(Response code %)" % (self.engine_guid, url,
                                               response.getcode()))
                raise BackupEngineException(msg)

        except Exception, e:
            raise BackupEngineException("Error while stopping flask server:"
                                        " %s" %e)


###############################################################################
# BackupEngineException
###############################################################################
class BackupEngineException(MBSException):

    ###########################################################################
    def __init__(self, message, cause=None):
        MBSException.__init__(self, message, cause=cause)