__author__ = 'abdul'


import traceback
import os

import time
import mbs_config
import logging

import urllib2

import persistence

from flask import Flask
from flask.globals import request
from globals import State, EventType
from threading import Thread
from multiprocessing import Process

from errors import (
    MBSError, BackupEngineError, EngineWorkerCrashedError,
    WorkspaceCreationError
)

from utils import (ensure_dir, resolve_path, get_local_host_name,
                   document_pretty_string, force_kill_process_and_children)

from mbs import get_mbs

from date_utils import timedelta_total_seconds, date_now, date_minus_seconds


from task import (
    EVENT_STATE_CHANGE, state_change_log_entry
)

from backup import Backup
from mbs_client.client import BackupEngineClient

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

# Failed one-off max due time (2 hours)
MAX_FAIL_DUE_TIME = 2 * 60 * 60

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

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
                       temp_dir=None,
                       command_port=8888):
        Thread.__init__(self)
        self._id = id
        self._engine_guid = None
        self._max_workers = int(max_workers)
        self._temp_dir = resolve_path(temp_dir or DEFAULT_BACKUP_TEMP_DIR_ROOT)
        self._command_port = command_port
        self._command_server = EngineCommandServer(self)
        self._tags = None
        self._resolved_tags = None
        self._stopped = False
        self._backup_processor = None
        self._restore_processor = None
        self._client = None


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
        self._tags = tags

    ###########################################################################
    def get_resolved_tags(self):
        if not self._resolved_tags and self.tags:
            self._resolved_tags = self._resolve_tags(self.tags)

        return self._resolved_tags

    ###########################################################################
    @property
    def command_port(self):
        return self._command_port

    @command_port.setter
    def command_port(self, command_port):
        self._command_port = command_port


    @property
    def client(self):
        if self._client is None:
            self._client = BackupEngineClient(api_url="http://0.0.0.0:%s" %
                                                      self.command_port)
        return self._client
    ###########################################################################
    def run(self):
        self.info("Starting up... ")
        self.info("PID is %s" % os.getpid())
        self.info("TEMP DIR is '%s'" % self.temp_dir)
        self.info("PATH is '%s'" % os.environ['PATH'])
        if self.get_resolved_tags():
            self.info("Tags are: %s" %
                      document_pretty_string(self.get_resolved_tags()))
        else:
            self.info("No tags configured")

        self._update_pid_file()
        # Start the command server
        self._start_command_server()

        self.start_task_processors()

        self.wait_task_processors()

        self.info("Engine completed")
        self._pre_shutdown()

    ###########################################################################
    def start_task_processors(self):
        # start the backup processor
        self.backup_processor.start()

        # start the restore processor
        self.restore_processor.start()

    ###########################################################################
    @property
    def backup_processor(self):
        if not self._backup_processor:
            bc = get_mbs().backup_collection
            self._backup_processor = TaskQueueProcessor("Backups", bc, self,
                                                        self._max_workers)
        return self._backup_processor


    ###########################################################################
    @property
    def restore_processor(self):
        if not self._restore_processor:
            rc = get_mbs().restore_collection
            self._restore_processor = TaskQueueProcessor("Restores", rc, self,
                                                         self._max_workers)
        return self._restore_processor

    ###########################################################################
    def wait_task_processors(self):
        # start the backup processor
        self.backup_processor.join()

        # start the restore processor
        self.restore_processor.join()

    ###########################################################################
    def _notify_error(self, exception):
        subject = "BackupEngine Error (%s)" % type(exception)
        message = ("BackupEngine '%s' Error!. Cause: %s. "
                   "\n\nStack Trace:\n%s" %
                   (self.engine_guid, exception, traceback.format_exc()))
        get_mbs().send_error_notification(subject, message, exception)


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
    def kill_engine_process(self):
        self.info("Attempting to kill engine process")
        pid = self._read_process_pid()
        if pid:
            self.info("Killing engine process '%s' using signal 9" % pid)
            force_kill_process_and_children(pid)
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
        return resolve_path(os.path.join(mbs_config.MBS_CONF_DIR, 
                                         pid_file_name))

    ###########################################################################
    def force_stop(self):
        self.kill_engine_process()

    ###########################################################################
    def cancel_backup(self, backup_id):
        backup = persistence.get_backup(backup_id)
        if not backup:
            raise BackupEngineError("Backup '%s' does not exist" % backup.id)
        self._cancel_task(backup, self.backup_processor)

    ###########################################################################
    def cancel_restore(self, restore_id):
        restore = persistence.get_restore(restore_id)
        if not restore:
            raise BackupEngineError("Restore '%s' does not exist" % restore.id)
        self._cancel_task(restore, self.restore_processor)

    ###########################################################################
    def _cancel_task(self, task, task_processor):
        if task.engine_guid != self.engine_guid:
            raise BackupEngineError("%s '%s' does not belong to this "
                                    "engine" % (task.type_name, task.id))

        elif task.state == State.CANCELED:
            # NO-OP
            pass
        elif task.state not in [State.FAILED, State.IN_PROGRESS]:
            raise BackupEngineError("Cannot cancel %s '%s' because its "
                                    "state '%s' is not FAILED or IN_PROGRESS" %
                                    (task.type_name, task.id, task.state))
        else:
            task_processor.cancel_task(task)

    ###########################################################################
    @property
    def worker_count(self):
        return (self.backup_processor.worker_count +
                self.restore_processor.worker_count)

    ###########################################################################
    def _do_stop(self):
        """
            Stops the engine gracefully by waiting for all workers to finish
            and not starting any new workers.
            Returns true if it will stop immediately (i.e. no workers running)
        """
        self.info("Stopping engine gracefully. Waiting for %s workers"
                  " to finish" % self.worker_count)

        self.backup_processor._stopped = True
        self.restore_processor._stopped = True
        return self.worker_count == 0

    ###########################################################################
    def _do_get_status(self):
        """
            Gets the status of the engine
        """
        if self.backup_processor._stopped:
            status = STATUS_STOPPING
        else:
            status = STATUS_RUNNING

        return {
            "status": status,
            "workers": {
                "backups": self.backup_processor.worker_count,
                "restores": self.restore_processor.worker_count
            },
            "versionInfo": get_mbs().get_version_info()
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


    ###########################################################################
    def _get_scheduled_tasks_query_hint(self, queue_processor):
        return None


###############################################################################
# TaskWorker
###############################################################################

class TaskQueueProcessor(Thread):
    ###########################################################################
    def __init__(self, name, task_collection, engine, max_workers=10):
        Thread.__init__(self)

        self._name = name
        self._task_collection = task_collection
        self._engine = engine
        self._sleep_time = 10
        self._stopped = False
        self._max_workers = int(max_workers)
        self._tick_count = 0
        self._workers = {}
        self._worker_id_seq = 0

    ###########################################################################
    def run(self):
        self._recover()

        while not self._stopped:
            try:
                self._tick()
            except Exception, e:
                self.error("Caught an error: '%s'.\nStack Trace:\n%s" %
                           (e, traceback.format_exc()))
                self._engine._notify_error(e)
            finally:
                time.sleep(self._sleep_time)

        ## wait for all workers to finish (if any)
        self._wait_for_running_workers()
        self.info("Exited main loop")

    ###########################################################################
    def _tick(self):
        # increase tick_counter
        self._tick_count += 1

        # try to start the next task if there are available workers
        if self._has_available_workers():
            self._start_next_task()

        # monitor workers
        self._monitor_workers()
        # Cancel a failed task every 5 ticks and there are available
        # workers
        if self._tick_count % 5 == 0 and self._has_available_workers():
            self._clean_next_past_due_failed_task()

    ###########################################################################
    def _wait_for_running_workers(self):
        self.info("Waiting for %s workers to finish" % self.worker_count)
        for worker in self._workers.values():
            worker.join()

        self.info("All workers finished!")

    ###########################################################################
    @property
    def worker_count(self):
        return len(self._workers)

    ###########################################################################
    @property
    def task_collection(self):
        return self._task_collection

    ###########################################################################
    def _start_next_task(self):
        task = self.read_next_task()
        if task:
            self._start_task(task)

    ###########################################################################
    def _monitor_workers(self):
        for worker in self._workers.values():
            if not worker.is_alive():
                # detect worker crashes
                if worker.exitcode != 0:
                    self.worker_crashed(worker)
                else:
                    self.info("Detected worker '%s' (pid %s, task id '%s') "
                              "finished successfully. Cleaning up resources..."
                              % (worker.id, worker.pid, worker.task_id))
                    self._cleanup_worker_resources(worker)

    ###########################################################################
    def _cleanup_worker_resources(self, worker):
        del self._workers[worker.id]

    ###########################################################################
    def cancel_task(self, task):
        worker = self._get_task_worker(task)
        if worker:
            self._cleanup_worker_resources(worker)
            # terminate the worker and all its children
            force_kill_process_and_children(worker.pid)
        self._clean_task(task)

    ###########################################################################
    def _get_task_worker(self, task):
        for worker in self._workers.values():
            if worker.task_id == task.id:
                return worker

    ###########################################################################
    def _clean_next_past_due_failed_task(self):

        # read next failed past due task
        task = self._read_next_failed_past_due_task()
        if task:
            # clean it
            worker = self._clean_task(task)
            self.info("Started clean task for task %s, TaskCleanWorker %s" %
                      (task.id, worker.id))

    ###########################################################################
    def _clean_task(self, task):
        worker = self._start_new_worker(task, TaskCleanWorker)
        return worker

    ###########################################################################
    def _start_task(self, task):
        self.info("Received  task %s" % task)
        worker = self._start_new_worker(task, TaskWorker)
        self.info("Started task %s, TaskWorker %s" %
                  (task.id, worker.id))

    ###########################################################################
    def _start_new_worker(self, task, worker_type):

        worker_id = self.next_worker_id()
        worker = worker_type(worker_id, task.id, self)
        self._workers[worker_id] = worker
        worker.start()
        return worker

    ###########################################################################
    def _has_available_workers(self):
        return self.worker_count < self._max_workers

    ###########################################################################
    def next_worker_id(self):
        self._worker_id_seq += 1
        return self._worker_id_seq

    ###########################################################################
    def worker_fail(self, worker, exception, trace=None):
        if isinstance(exception, MBSError):
            log_msg = exception.message
        else:
            log_msg = "Unexpected error. Please contact admin"

        details = "%s. Stack Trace: %s" % (exception, trace)
        task = worker.get_task()
        self.task_collection.update_task(
            task, event_type=EventType.ERROR,
            message=log_msg, details=details)

        self.worker_finished(worker, task, State.FAILED)

        nh = get_mbs().notification_handler
        # send a notification only if the task is not reschedulable
        if not task.reschedulable and nh:
            nh.notify_on_task_failure(task, exception, trace)

    ###########################################################################
    def worker_crashed(self, worker):
        # page immediately
        subject = "Backup Worker crashed!"
        message = ("Backup worker crashed on engine '%s'" % self._engine.id)

        errmsg = ("Worker crash detected! Worker (id %s, pid %s, task"
                  " id '%s') finished with a non-zero exit code '%s'"
                  % (worker.id, worker.pid, worker.task_id,
                  worker.exitcode))

        exception = EngineWorkerCrashedError(errmsg)
        get_mbs().send_error_notification(subject, message, exception)

        self.error(errmsg)
        self._cleanup_worker_resources(worker)
        self.worker_fail(worker, exception)

    ###########################################################################
    def worker_success(self, worker):
        task = worker.get_task()
        self.task_collection.update_task(
            task,
            message="Task completed successfully!")

        self.worker_finished(worker, task, State.SUCCEEDED)

    ###########################################################################
    def cleaner_finished(self, worker):
        task = worker.get_task()
        self.worker_finished(worker, task, State.CANCELED)

    ###########################################################################
    def worker_finished(self, worker, task, state, message=None):

        # set end date
        task.end_date = date_now()
        task.state = state
        self.task_collection.update_task(
            task, properties=["state", "endDate"],
            event_name=EVENT_STATE_CHANGE, message=message)

    ###########################################################################
    def _recover(self):
        """
        Does necessary recovery work on crashes. Fails all tasks that crashed
        while in progress and makes them reschedulable. Backup System will
        decide to cancel them or reschedule them.
        """
        self.info("Running recovery..")

        q = {
            "state": State.IN_PROGRESS,
            "engineGuid": self._engine.engine_guid
        }

        total_crashed = 0
        msg = ("Engine crashed while task was in progress. Failing...")
        for task in self.task_collection.find(q):
            # fail task
            self.info("Recovery: Failing task %s" % task.id)
            task.reschedulable = True
            task.state = State.FAILED
            task.end_date = date_now()
            # update
            self.task_collection.update_task(
                task, properties=["state", "reschedulable", "endDate"],
                event_type=EVENT_STATE_CHANGE, message=msg)

            total_crashed += 1

        self.info("Recovery complete! Total Crashed task: %s." %
                  total_crashed)

    ###########################################################################
    def read_next_task(self):

        log_entry = state_change_log_entry(State.IN_PROGRESS)
        q = self._get_scheduled_tasks_query()
        u = {"$set" : { "state": State.IN_PROGRESS,
                        "engineGuid": self._engine.engine_guid},
             "$push": {"logs":log_entry.to_document()}}

        # sort by priority except every third tick, we sort by created date to
        # avoid starvation
        if self._tick_count % 5 == 0:
            s = [("createdDate", 1)]
        else:
            s = [("priority", 1)]

        c = self.task_collection

        hint = self._engine._get_scheduled_tasks_query_hint(self)

        task = c.find_and_modify(query=q, sort=s, update=u, new=True, hint=hint)

        return task

    ###########################################################################
    def _read_next_failed_past_due_task(self):
        min_fail_end_date = date_minus_seconds(date_now(), MAX_FAIL_DUE_TIME)
        q = { "state": State.FAILED,
              "engineGuid": self._engine.engine_guid,
              "$or": [
                      {
                      "plan.nextOccurrence": {"$lte": date_now()}
                  },

                      {
                      "plan": {"$exists": False},
                      "reschedulable": False,
                      "endDate": {"$lte": min_fail_end_date}
                  }


              ]
        }

        msg = "Task failed and is past due. Cancelling..."
        log_entry = state_change_log_entry(State.CANCELED, message=msg)
        u = {"$set" : { "state" : State.CANCELED},
             "$push": {
                 "logs": log_entry.to_document()
             }
        }

        return self.task_collection.find_and_modify(query=q, update=u,
                                                    new=True)

    ###########################################################################
    def _get_scheduled_tasks_query(self):
        q = {"state": State.SCHEDULED}
        tags = self._engine.get_resolved_tags()
        # add tags if specified
        if tags:
            tag_filters = []
            for name, value in tags.items():
                tag_prop_path = "tags.%s" % name
                tag_filters.append({tag_prop_path: value})

            q["$or"] = tag_filters

        return q

    ###########################################################################
    # Logging methods
    ###########################################################################
    def info(self, msg):
        self._engine.info("%s Task Processor: %s" % (self._name, msg))

    ###########################################################################
    def warning(self, msg):
        self._engine.info("%s Task Processor: %s" % (self._name, msg))

    ###########################################################################
    def error(self, msg):
        self._engine.info("%s Task Processor: %s" % (self._name, msg))

###############################################################################
# TaskWorker
###############################################################################

class TaskWorker(Process):

    ###########################################################################
    def __init__(self, id, task_id, processor):
        Process.__init__(self)
        self._id = id
        self._task_id = task_id
        self._processor = processor

    ###########################################################################
    @property
    def task_id(self):
        return self._task_id

    ###########################################################################
    def get_task(self):
        return self._processor.task_collection.get_by_id(self.task_id)

    ###########################################################################
    @property
    def id(self):
        return self._id

    ###########################################################################
    @property
    def processor(self):
        return self._processor

    ###########################################################################
    def run(self):
        task = self.get_task()

        try:
            # increase # of tries
            task.try_count += 1

            self.info("Running task '%s' (try # %s) (worker PID '%s')..." %
                      (task.id, task.try_count, self.pid))
            # set start date
            task.start_date = date_now()

            # set queue_latency_in_minutes if its not already set
            if not task.queue_latency_in_minutes:
                latency = self._calculate_queue_latency(task)
                task.queue_latency_in_minutes = latency

            # clear end date
            task.end_date = None

            # set the workspace its its not set
            if not task.workspace:
                workspace_dir = self._get_task_workspace_dir(task)
                task.workspace = workspace_dir

            # UPDATE!
            self._processor.task_collection.update_task(
                task, properties=["tryCount", "startDate", "endDate",
                                  "workspace", "queueLatencyInMinutes"])

            # run the task
            task.execute()

            # cleanup temp workspace
            task.cleanup()

            # success!
            self._processor.worker_success(self)

            self.info("Task '%s' completed successfully" % task.id)

        except Exception, e:
            # fail
            trace = traceback.format_exc()
            self.error("Task failed. Cause %s. \nTrace: %s" % (e, trace))
            self._processor.worker_fail(self, exception=e, trace=trace)


    ###########################################################################
    def _get_task_workspace_dir(self, task):
        return os.path.join(self._processor._engine.temp_dir, str(task.id))

    ###########################################################################
    def _calculate_queue_latency(self, task):
        if isinstance(task, Backup):
            occurrence_date = task.plan_occurrence or task.created_date
        else:
            occurrence_date = task.created_date

        latency_secs = timedelta_total_seconds(task.start_date -
                                               occurrence_date)

        return round(latency_secs/60, 2)

    ###########################################################################
    def info(self, msg):
        self._processor.info("Worker-%s: %s" % (self.id, msg))

    ###########################################################################
    def warning(self, msg):
        self._processor.warning("Worker-%s: %s" % (self.id, msg))

    ###########################################################################
    def error(self, msg):
        self._processor.error("Worker-%s: %s" % (self.id, msg))


###############################################################################
# TaskCleanWorker
###############################################################################

class TaskCleanWorker(TaskWorker):

    ###########################################################################
    def __init__(self, id, task_id, engine):
        TaskWorker.__init__(self, id, task_id, engine)

    ###########################################################################
    def run(self):
        try:
            task = self.get_task()
            task.cleanup()
        finally:
            self._processor.cleaner_finished(self)

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
                            engine.worker_count)
            except Exception, e:
                return "Error while trying to stop engine: %s" % e

        ## build status method
        @flask_server.route('/status', methods=['GET'])
        def status():
            logger.info("Command Server: Received a status command")
            try:
                return document_pretty_string(engine._do_get_status())
            except Exception, e:
                msg = "Error while trying to get engine status: %s" % e
                logger.error(msg)
                logger.error(traceback.format_exc())
                return {
                    "status": "UNKNOWN",
                    "error": msg
                }

        ## build cancel-backup method
        @flask_server.route('/cancel-backup',
                            methods=['POST'])
        def cancel_backup():
            backup_id = request.args.get('backupId')
            logger.info("Command Server: Received a cancel-backup command")
            try:
                engine.cancel_backup(backup_id)
                return document_pretty_string({
                    "ok": 1
                })
            except Exception, e:
                msg = ("Error while trying to cancel backup '%s': %s" %
                      (backup_id, e))
                logger.error(msg)
                logger.error(traceback.format_exc())
                return document_pretty_string({
                    "ok": 0,
                    "error": msg
                })

        ## build cancel-backup method
        @flask_server.route('/cancel-restore',
                            methods=['POST'])
        def cancel_restore():
            restore_id = request.args.get('restoreId')
            logger.info("Command Server: Received a cancel-restore command")
            try:
                engine.cancel_restore(restore_id)
                return document_pretty_string({
                    "ok": 1
                })
            except Exception, e:
                msg = ("Error while trying to cancel restore '%s': %s" %
                       (restore_id, e))
                logger.error(msg)
                logger.error(traceback.format_exc())
                return document_pretty_string({
                    "ok": 0,
                    "error": msg
                })

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
        self._flask_server.run(host="0.0.0.0", port=self._engine.command_port,
                               threaded=True)

    ###########################################################################
    def stop(self):

        logger.info("EngineCommandServer: Stopping flask server ")
        port = self._engine.command_port
        url = "http://0.0.0.0:%s/stop-command-server" % port
        try:
            response = urllib2.urlopen(url, timeout=30)
            if response.getcode() == 200:
                logger.info("EngineCommandServer: Flask server stopped "
                            "successfully")
                return response.read().strip()
            else:
                msg = ("Error while trying to get status engine '%s' URL %s "
                       "(Response code %s)" % (self._engine.engine_guid, url,
                                               response.getcode()))
                raise BackupEngineError(msg)

        except Exception, e:
            raise BackupEngineError("Error while stopping flask server:"
                                    " %s" % e)
