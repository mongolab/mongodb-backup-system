__author__ = 'abdul'


import traceback
import os

import time
import mbs_logging

import urllib

import json

from flask import Flask
from flask.globals import request

from threading import Thread


from errors import MBSError

from utils import (ensure_dir, resolve_path, get_local_host_name,
                   document_pretty_string)

from mbs import MBS_CONF_DIR, get_mbs

from date_utils import  timedelta_total_seconds, date_now


from backup import (STATE_SCHEDULED, STATE_IN_PROGRESS, STATE_FAILED,
                    STATE_SUCCEEDED, STATE_CANCELED,
                    EVENT_TYPE_ERROR, EVENT_STATE_CHANGE, state_change_log_entry)

from persistence import update_backup

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
    def __init__(self, id=None, max_workers=10,
                       sleep_time=10,
                       temp_dir=None,
                       notification_handler=None,
                       command_port=8888):
        Thread.__init__(self)
        self._id = id
        self._engine_guid = None
        self._sleep_time = sleep_time
        self._worker_count = 0
        self._max_workers = int(max_workers)
        self._temp_dir = resolve_path(temp_dir or DEFAULT_BACKUP_TEMP_DIR_ROOT)
        self._notification_handler = notification_handler
        self._stopped = False
        self._command_port = command_port
        self._command_server = EngineCommandServer(self)
        self._tags = None
        self._tick_count = 0

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, val):
        if val:
            self._id = val.encode('ascii', 'ignore')

    ###########################################################################
    @property
    def engine_guid(self):
        if not self._engine_guid:
            self._engine_guid = get_local_host_name() + "-" + self.id
        return self._engine_guid

    ###########################################################################
    @property
    def backup_collection(self):
        return get_mbs().backup_collection

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
        self.info("PID is %s" % os.getpid())
        self.info("TEMP DIR is '%s'" % self.temp_dir)
        if self.tags:
            self.info("Tags are: %s" % document_pretty_string(self.tags))
        else:
            self.info("No tags configured")

        ensure_dir(self._temp_dir)
        self._update_pid_file()
        # Start the command server
        self._start_command_server()
        self._recover()

        while not self._stopped:
            try:
                self._tick()
                time.sleep(self._sleep_time)
            except Exception, e:
                self.error("Caught an error: '%s'.\nStack Trace:\n%s" %
                           (e, traceback.format_exc()))
                self._notify_error(e)

        self.info("Exited main loop")
        self._pre_shutdown()

    ###########################################################################
    def _tick(self):
        # increase tick_counter
        self._tick_count += 1

        # try to start the next backup if there are available workers
        if self._has_available_workers():
            self._start_next_backup()

        # Cancel a failed backup every 100 ticks and there are available
        # workers
        if self._tick_count % 100 == 0 and self._has_available_workers():
            self._clean_next_past_due_failed_backup()

    ###########################################################################
    def _start_next_backup(self):
        backup = self.read_next_backup()
        if backup:
            self._start_backup(backup)

    ###########################################################################
    def _clean_next_past_due_failed_backup(self):

        # read next failed past due backup
        backup = self._read_next_failed_past_due_backup()
        if backup:
            # clean it
            worker_id = self.next_worker_id()
            self.info("Starting cleaner worker for backup '%s'" % backup.id)
            BackupCleanerWorker(worker_id, backup, self).start()

    ###########################################################################
    def _start_backup(self, backup):
        self.info("Received  backup %s" % backup)
        worker_id = self.next_worker_id()
        self.info("Starting backup %s, BackupWorker %s" %
                  (backup._id, worker_id))
        BackupWorker(worker_id, backup, self).start()

    ###########################################################################
    def _has_available_workers(self):
        return self._worker_count < self.max_workers

    ###########################################################################
    def next_worker_id(self):
        self._worker_count+= 1
        return self._worker_count

    ###########################################################################
    def worker_fail(self, worker, exception, trace=None):
        if isinstance(exception, MBSError):
            log_msg = exception.message
        else:
            log_msg = "Unexpected error. Please contact admin"

        details = "%s. Stack Trace: %s" % (exception, trace)
        update_backup(worker.backup, event_type=EVENT_TYPE_ERROR,
                      message=log_msg, details=details)

        self.worker_finished(worker, STATE_FAILED)

        backup = worker.backup
        # send a notification only if the backup is not reschedulable
        if not backup.reschedulable and self.notification_handler:
            subject = "Backup failed"
            message = ("Backup '%s' failed.\n%s\n\nCause: \n%s\nStack Trace:"
                       "\n%s" % (backup.id, backup, exception, trace))

            nh = self.notification_handler
            nh.notify_on_backup_failure(backup, exception, trace)

    ###########################################################################
    def _notify_error(self, exception):
        subject = "BackupEngine Error"
        message = ("BackupEngine '%s' Error!. Cause: %s. "
                   "\n\nStack Trace:\n%s" %
                   (self.engine_guid, exception, traceback.format_exc()))
        self._send_error_notification(subject, message, exception)

    ###########################################################################
    def _send_notification(self, subject, message):
        if self.notification_handler:
            self.notification_handler.send_notification(subject, message)

    ###########################################################################
    def _send_error_notification(self, subject, message, exception):
        if self.notification_handler:
            self.notification_handler.send_error_notification(subject, message,
                                                              exception)
    ###########################################################################
    def worker_success(self, worker):
        update_backup(worker.backup,
                      message="Backup completed successfully!")

        self.worker_finished(worker, STATE_SUCCEEDED)

    ###########################################################################
    def cleaner_finished(self, worker):
        self.worker_finished(worker, STATE_CANCELED)

    ###########################################################################
    def worker_finished(self, worker, state, message=None):
        backup = worker.backup
        # set end date if not set already
        if not backup.end_date:
            backup.end_date = date_now()
        # decrease worker count and update state
        self._worker_count -= 1
        backup.state = state
        update_backup(backup, properties=["state", "endDate"],
                      event_name=EVENT_STATE_CHANGE, message=message)

    ###########################################################################
    def _recover(self):
        """
        Does necessary recovery work on crashes. Fails all backups that crashed
        while in progress and makes them reschedulable. Plan manager will
        decide to cancel them or reschedule them.
        """
        self.info("Running recovery..")

        q = {
            "state": STATE_IN_PROGRESS,
            "engineGuid": self.engine_guid
        }

        total_crashed = 0
        msg = ("Engine crashed while backup was in progress. Failing...")
        for backup in self.backup_collection.find(q):
            # fail backup
            self.info("Recovery: Failing backup %s" % backup._id)
            backup.reschedulable = True
            backup.state = STATE_FAILED
            # update
            update_backup(backup, properties=["state", "reschedulable"],
                          event_type=EVENT_STATE_CHANGE, message=msg)

            total_crashed += 1



        self.info("Recovery complete! Total Crashed backups: %s." %
                  total_crashed)

    ###########################################################################
    def read_next_backup(self):

        log_entry = state_change_log_entry(STATE_IN_PROGRESS)
        q = self._get_scheduled_backups_query()
        u = {"$set" : { "state" : STATE_IN_PROGRESS,
                        "engineGuid": self.engine_guid},
             "$push": {"logs":log_entry.to_document()}}

        # sort by priority except every third tick, we sort by created date to
        # avoid starvation
        if self._tick_count % 5 == 0:
            s = [("createdDate", 1)]
        else:
            s = [("priority", 1)]

        c = self.backup_collection

        backup = c.find_and_modify(query=q, sort=s, update=u, new=True)

        return backup

    ###########################################################################
    def _read_next_failed_past_due_backup(self):
        q = { "state": STATE_FAILED,
              "engineGuid": self.engine_guid,
              "$or": [
                  {
                      "plan.nextOccurrence": {"$lte": date_now()}
                  },

                  {
                      "plan": {"$exists": False},
                       "reschedulable": False
                  }


              ]
        }

        msg = "Backup failed and is past due. Cancelling..."
        log_entry = state_change_log_entry(STATE_CANCELED, message=msg)
        u = {"$set" : { "state" : STATE_CANCELED},
             "$push": {
                 "logs": log_entry.to_document()
             }
        }

        return self.backup_collection.find_and_modify(query=q, update=u,
                                                       new=True)

    ###########################################################################
    def _get_scheduled_backups_query(self):
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
        # if value is not a string then return it as is
        if not isinstance(value, (str, unicode)):
            return value
        for binding_name, binding_value in self._get_tag_bindings().items():
            value = value.replace(binding_name, binding_value)

        return value

    ###########################################################################
    def _kill_engine_process(self):
        self.info("Attempting to kill engine process")
        pid = self._read_process_pid()
        if pid:
            self.info("Killing engine process '%s' using signal 9" % pid)
            os.kill(int(pid), 9)
        else:
            raise BackupEngineError("Unable to determine engine process id")

    ###########################################################################
    def _update_pid_file(self):
        pid_file = open(self._get_pid_file_path(), 'w')
        pid_file.write(str(os.getpid()))
        pid_file.close()

    ###########################################################################
    def _read_process_pid(self):
        pid_file = open(self._get_pid_file_path(), 'r')
        pid = pid_file.read()
        if pid:
            return int(pid)

    ###########################################################################
    def _get_pid_file_path(self):
        pid_file_name = "engine_%s_pid.txt" % self.id
        return resolve_path(os.path.join(MBS_CONF_DIR, pid_file_name))

    ###########################################################################
    # Engine stopping
    ###########################################################################
    def stop(self, force=False):
        """
            Sends a stop request to the engine using the command port
            This should be used by other processes (copies of the engine
            instance) but not the actual running engine process
        """

        if force:
            self._kill_engine_process()
            return

        url = "http://0.0.0.0:%s/stop" % self.command_port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                print response.read().strip()
            else:
                msg =  ("Error while trying to stop engine '%s' URL %s "
                        "(Response"" code %)" %
                        (self.engine_guid, url, response.getcode()))
                raise BackupEngineError(msg)
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
                return json.loads(response.read().strip())
            else:
                msg =  ("Error while trying to get status engine '%s' URL %s "
                        "(Response code %)" % (self.engine_guid, url,
                                               response.getcode()))
                raise BackupEngineError(msg)

        except IOError, ioe:
            return {
                    "status":STATUS_STOPPED
                }

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
            status = STATUS_STOPPING
        else:
            status = STATUS_RUNNING

        return {
            "status": status,
            "workers": self._worker_count
        }

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
    def warning(self, msg):
        logger.warning("<BackupEngine-%s>: %s" % (self.id, msg))

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

        try:
            # increase # of tries
            backup.try_count += 1

            self.info("Running %s backup %s (try # %s)" %
                      (backup.strategy, backup._id, backup.try_count))
            # set start date
            backup.start_date = date_now()
            # clear end date
            backup.end_date = None

            # set backup name
            _set_backup_name(backup)

            # set the workspace
            workspace_dir = self._get_backup_workspace_dir(backup)
            backup.workspace = workspace_dir

            # UPDATE!
            update_backup(backup, properties=["tryCount", "startDate",
                                              "endDate", "name", "workspace"])
            # apply the retention policy
            # TODO Probably should be called somewhere else
            self._apply_retention_policy(backup)

            # run the backup
            self.backup.strategy.run_backup(backup)



            # cleanup temp workspace
            self.backup.strategy.cleanup_backup(backup)

            # calculate backup rate
            self._calculate_backup_rate(backup)

            # success!
            self.engine.worker_success(self)

            self.info("Backup '%s' completed successfully" % backup.id)

        except Exception, e:
            # fail
            trace = traceback.format_exc()
            self.error("Backup failed. Cause %s. \nTrace: %s" % (e, trace))
            self.engine.worker_fail(self, exception=e, trace=trace)
        finally:
            # apply the retention policy
            # TODO Probably should be called somewhere else
            self._apply_retention_policy(backup)


    ###########################################################################
    def _get_backup_workspace_dir(self, backup):
        return os.path.join(self.engine.temp_dir, str(backup._id))

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
    def _apply_retention_policy(self, backup):
        """
            apply the backup plan's retention policy if any.
            No retention policies for one offs
        """
        try:
            plan = backup.plan
            if plan and plan.retention_policy:
                plan.retention_policy.apply_policy(plan)
        except Exception, e:
            msg = ("Error while applying retention policy for backup plan "
                   "'%s'. %s" % (backup.plan.id, e))
            logger.error(msg)
            self.engine._send_error_notification("Retention Policy Error", msg,
                                                  e)


    ###########################################################################
    def info(self, msg):
        self._engine.info("Worker-%s: %s" % (self._id, msg))

    ###########################################################################
    def warning(self, msg):
        self._engine.warning("Worker-%s: %s" % (self._id, msg))

    ###########################################################################
    def error(self, msg):
        self._engine.error("Worker-%s: %s" % (self._id, msg))


###############################################################################
# BackupCleanerWorker
###############################################################################

class BackupCleanerWorker(BackupWorker):

    ###########################################################################
    def __init__(self, id, backup, engine):
        BackupWorker.__init__(self, id, backup, engine)

    ###########################################################################
    def run(self):
        try:
            self.backup.strategy.cleanup_backup(self.backup)
        finally:
            self.engine.cleaner_finished(self)

###############################################################################
def _set_backup_name(backup):
    if not backup.name:
        if backup.plan:
            backup.name = backup.plan.get_backup_name(backup)
        else:
            backup.name = str(backup.id)

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
                return document_pretty_string(engine._do_get_status())
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
                raise BackupEngineError(msg)

        except Exception, e:
            raise BackupEngineError("Error while stopping flask server:"
                                        " %s" %e)


###############################################################################
# BackupEngineError
###############################################################################
class BackupEngineError(MBSError):
    pass