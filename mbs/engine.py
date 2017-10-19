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


from errors import (
    MBSError, BackupEngineError, EngineWorkerCrashedError,
    to_mbs_error_code
)

from utils import (resolve_path, get_local_host_name, safe_stringify,
                   document_pretty_string, force_kill_process_and_children, which, ensure_dir)

from mbs import get_mbs

from date_utils import timedelta_total_seconds, date_now, date_minus_seconds


from task import (
    EVENT_STATE_CHANGE, state_change_log_entry
)

from backup import Backup
from restore import Restore
from mbs_client.client import BackupEngineClient

from task_utils import set_task_retry_info, trigger_task_finished_event

import subprocess
from schedule_runner import ScheduleRunner
from schedule import Schedule

###############################################################################
# CONSTANTS
###############################################################################

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
                       command_port=8888):
        Thread.__init__(self)
        self._id = id
        self._engine_guid = None
        self._max_workers = int(max_workers)
        self._sleep_time = 25
        self._command_port = command_port
        self._command_server = EngineCommandServer(self)
        self._tags = None
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
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        tags = tags or {}
        self._tags = tags

    ###########################################################################
    @property
    def command_port(self):
        return self._command_port

    @command_port.setter
    def command_port(self, command_port):
        self._command_port = command_port


    ###########################################################################
    @property
    def sleep_time(self):
        return self._sleep_time

    @sleep_time.setter
    def sleep_time(self, val):
        self._sleep_time = val

    ###########################################################################
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
        self.info("PATH is '%s'" % os.environ['PATH'])
        if self.tags:
            self.info("Tags are: %s" % document_pretty_string(self.tags))
        else:
            self.info("No tags configured")

        # ensure task log dirs
        ensure_dir(task_log_dir("backup"))
        ensure_dir(task_log_dir("restore"))

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
            self._backup_processor = TaskQueueProcessor("Backups", Backup().type_name, "backups", self,
                                                        self._max_workers, sleep_time=self.sleep_time)
        return self._backup_processor

    ###########################################################################
    @property
    def restore_processor(self):
        if not self._restore_processor:
            self._restore_processor = TaskQueueProcessor("Restores", Restore().type_name, "restores", self,
                                                         self._max_workers, sleep_time=self.sleep_time)
        return self._restore_processor

    ###########################################################################
    def get_task_collection_by_name(self, name):
        if name == "backups":
            return get_mbs().backup_collection
        elif name == "restores":
            return get_mbs().restore_collection

    ###########################################################################
    def wait_task_processors(self):
        # start the backup processor
        self.backup_processor.join()

        # start the restore processor
        self.restore_processor.join()

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
        return resolve_path(os.path.join("~/.mbs", pid_file_name))

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


###############################################################################
# TaskQueueProcessor
###############################################################################

class TaskQueueProcessor(Thread):
    ###########################################################################
    def __init__(self, name, task_type_name, task_collection_name, engine, max_workers=10, sleep_time=25):
        Thread.__init__(self)

        self._name = name
        self._task_collection_name = task_collection_name
        self._engine = engine
        self._sleep_time = sleep_time
        self._stopped = False
        self._max_workers = int(max_workers)
        self._tick_count = 0
        self._workers = {}
        self._log_file_sweeper = TaskLogFileSweeper(task_type_name)

    ###########################################################################
    def run(self):
        self._log_file_sweeper.start()
        self._recover()

        while not self._stopped:
            try:
                self._tick()
            except Exception, e:
                self.error("Caught an error: '%s'.\nStack Trace:\n%s" %
                           (e, traceback.format_exc()))
            finally:
                time.sleep(self._sleep_time)

        ## wait for all workers to finish (if any)
        self._wait_for_running_workers()
        self._log_file_sweeper.stop(True)
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
        self._monitor_cancel_requests()
        # Cancel a failed task every 40 ticks and there are available
        # workers
        if self._tick_count % 40 == 0 and self._has_available_workers():
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
        # NOTE we are not memoizing the collection to ensure that the we get a process local copy.
        # This is to avoid pymongo3 vs multiprocessing issue since this TaskQueueProcessor object will be
        # copied down to the subprocess
        return self._engine.get_task_collection_by_name(self._task_collection_name)

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
                if worker.exit_code != 0:
                    self.worker_crashed(worker)
                else:
                    self.info("Detected worker '%s' (pid %s, task id '%s') "
                              "finished successfully. Cleaning up resources..."
                              % (worker.id, worker.pid, worker.task.id))
                    self._cleanup_worker_resources(worker)

    ###########################################################################
    def _monitor_cancel_requests(self):
        for worker in self._workers.values():
            task = worker.task
            latest_task = self.task_collection.find_one(task.id)
            if latest_task.cancel_requested_at:
                self.cancel_task(latest_task)

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
            if worker.task.id == task.id:
                return worker

    ###########################################################################
    def _clean_next_past_due_failed_task(self):

        # read next failed past due task
        task = self._read_next_failed_past_due_task()
        if task:
            # clean it
            worker = self._clean_task(task)
            self.info("Started cleanup for %s %s" % (task.type_name, task.id))

    ###########################################################################
    def _clean_task(self, task):
        worker = self._start_new_worker(task, cleaner=True)
        return worker

    ###########################################################################
    def _start_task(self, task):
        self.info("Received %s %s" % (task.type_name, task.id))
        worker = self._start_new_worker(task)
        self.info("Started %s %s (worker %s)" % (task.type_name, task.id, worker.pid))

    ###########################################################################
    def _start_new_worker(self, task, cleaner=False):

        if not cleaner:
            worker = TaskWorker(task, env_vars=self._engine.get_task_worker_env_vars())
        else:
            worker = TaskCleanWorker(task, env_vars=self._engine.get_task_worker_env_vars())

        worker.start()
        self._workers[worker.id] = worker

        return worker

    ###########################################################################
    def _has_available_workers(self):
        return self.worker_count < self._max_workers

    ###########################################################################
    def worker_crashed(self, worker):
        # page immediately
        subject = "Worker crashed for %s %s!" % (worker.task.type_name, worker.task.id)

        errmsg = ("Worker crash detected! Worker (id %s, pid %s, %s"
                  " id '%s') finished with a non-zero exit code '%s'"
                  % (worker.id, worker.pid, worker.task.type_name, worker.task.id, worker.exit_code))

        exception = EngineWorkerCrashedError(errmsg)
        get_mbs().notifications.send_error_notification(subject, errmsg)

        self.error(errmsg)
        self._cleanup_worker_resources(worker)
        worker.worker_fail(exception)

    ###########################################################################
    def _recover(self):
        """
        Does necessary recovery work on crashes. Fails all tasks that crashed
        while in progress and makes them reschedulable. Backup System will
        decide to cancel them or reschedule them.
        """
        self.info("Running recovery..")


        total_crashed = 0

        # recover crashed tasks in state IN PROGRESS
        for task in self.task_collection.find({
            "state": State.IN_PROGRESS,
            "engineGuid": self._engine.engine_guid
        }):
            msg = ("Engine crashed while %s %s was in progress. Recovering..." % (task.type_name, task.id))
            self.info("Recovery: Recovering %s %s" % (task.type_name, task.id))

            # update
            self.task_collection.update_task(task, message=msg)

            self._start_task(task)

            total_crashed += 1

        # recover crashed tasks in state FAILED, those are the ones that crashed right before engine restart
        for task in self.task_collection.find({
            "state": State.FAILED,
            "engineGuid": self._engine.engine_guid,
        }):
            last_error = task.get_last_error()
            if isinstance(last_error, EngineWorkerCrashedError):
                msg = ("Engine crashed while %s %s was in progress. Recovering..." % (task.type_name, task.id))
                self.info("Recovery: Recovering %s %s" % (task.type_name, task.id))

                # update
                task.state = State.IN_PROGRESS
                self.task_collection.update_task(task, properties="state", message=msg)

                self._start_task(task)

                total_crashed += 1

        self.info("Recovery complete! Total Crashed task: %s." %
                  total_crashed)

    ###########################################################################
    def read_next_task(self):

        log_entry = state_change_log_entry(State.IN_PROGRESS)
        q = self._get_scheduled_tasks_query()
        u = {
            "$set": {
                "state": State.IN_PROGRESS,
                "engineGuid": self._engine.engine_guid
            },
            "$push": {
                "logs": log_entry.to_document()
            }
        }

        # Ensure that engines will not pickup tasks that were already processed by other engines
        if self._tick_count % 2 == 0:
            q["engineGuid"] = self._engine.engine_guid
        else:
            q["engineGuid"] = None

        # sort by priority except every third tick, we sort by created date to
        # avoid starvation
        if self._tick_count % 5 == 0:
            s = [("createdDate", 1)]
        else:
            s = [("priority", 1)]

        c = self.task_collection

        task = c.find_one(query=q, sort=s)
        if task:
            if task.engine_guid and task.engine_guid != self._engine.engine_guid:
                raise Exception("Unexpected error")
            else:
                task = c.find_and_modify(query=q, sort=s, update=u, new=True)

        return task

    ###########################################################################
    def _read_next_failed_past_due_task(self):
        min_fail_end_date = date_minus_seconds(date_now(), MAX_FAIL_DUE_TIME)
        q = {
            "$or": [
                {
                    "state": State.FAILED,
                    "engineGuid": self._engine.engine_guid,
                    "nextRetryDate": None,
                    "finalRetryDate": {"$lte": date_now()},
                    "plan.nextOccurrence": {"$lte": date_now()}
                },
                {
                    "state": State.FAILED,
                    "engineGuid": self._engine.engine_guid,
                    "plan": {"$exists": False},
                    "nextRetryDate": None,
                    "finalRetryDate": {"$lte": min_fail_end_date}
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

        task = self.task_collection.find_one(query=q)
        if task:
            return self.task_collection.find_and_modify(query=q, update=u, new=True)

    ###########################################################################
    def _get_scheduled_tasks_query(self):
        q = {"state": State.SCHEDULED}
        tags = self._engine.tags
        # add tags if specified
        if tags:
            for name, value in tags.items():
                tag_prop_path = "tags.%s" % name
                q[tag_prop_path] = value

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

    ###########################################################################
    def get_task_worker_env_vars(self):
        """
        to override as needed
        :return:
        """
        return None

###############################################################################
# TaskWorker
###############################################################################

class TaskWorker(object):

    ###########################################################################
    def __init__(self, task, env_vars=None):
        self._task = task
        self._popen = None
        self._id = None
        self._env_vars = None

    ###########################################################################
    @property
    def task(self):
        return self._task

    ###########################################################################
    @property
    def pid(self):
        if self._popen:
            return self._popen.pid

    ###########################################################################
    @property
    def id(self):
        return self._id

    ###########################################################################
    @property
    def pid(self):
        if self._popen:
            return self._popen.pid

    ###########################################################################
    def get_task_collection(self):
        if isinstance(self._task, Backup):
            return get_mbs().backup_collection
        elif isinstance(self._task, Restore):
            return get_mbs().restore_collection

    ###########################################################################
    def get_cmd(self):
        return "run-%s" % self._task.type_name.lower()

    ###########################################################################
    def start(self):
        cmd = self.get_cmd()
        run_task_command = [
            which("mbs"),
            "--config-path",
            mbs_config.MBS_CONF_PATH,
            cmd,
            str(self._task.id)
        ]

        log_file_path = self.get_log_path()
        ensure_dir(os.path.dirname(log_file_path))

        log_file = open(log_file_path, "a")
        child_env_var = os.environ.copy()
        if self._env_vars:
            child_env_var.update(self._env_vars)
        self._popen = subprocess.Popen(run_task_command, stdout=log_file, stderr=subprocess.STDOUT,
                                       env=child_env_var)
        self._id = str(self._popen.pid)

    ###########################################################################
    def get_log_path(self):
        log_dir = resolve_path(os.path.join(mbs_config.MBS_LOG_PATH, self._task.type_name.lower() + "s"))

        log_file_name = "%s-%s.log" % (self._task.type_name.lower(), str(self._task.id))
        log_file_path = os.path.join(log_dir, log_file_name)
        return log_file_path

    ###########################################################################
    def join(self):
        self._popen.wait()

    ###########################################################################
    @property
    def exit_code(self):
        if self._popen:
            if self._popen.returncode:
                return self._popen.returncode
            else:
                return self._popen.poll()

    ###########################################################################
    def is_alive(self):
        return self.exit_code is None

    ###########################################################################
    def run(self):
        task = self._task

        try:
            # increase # of tries
            task.try_count += 1

            logger.info("Running %s '%s' (try # %s) (worker PID '%s')..." %
                      (task.type_name, task.id, task.try_count, os.getpid()))

            logger.info(str(task))

            # set start date
            task.start_date = date_now()

            task.worker_info = self.get_worker_info()

            # set queue_latency_in_minutes if its not already set
            if not task.queue_latency_in_minutes:
                latency = self._calculate_queue_latency(task)
                task.queue_latency_in_minutes = latency

            # clear end date
            task.end_date = None

            # UPDATE!
            self.get_task_collection().update_task(
                task, properties=["tryCount", "startDate", "endDate", "queueLatencyInMinutes", "workerInfo"])

            # run the task
            task.execute()

            # cleanup temp workspace
            task.cleanup()

            # success!
            self.worker_success()

            logger.info("%s '%s' completed successfully" % (task.type_name, task.id))

        except Exception, e:
            # fail
            trace = traceback.format_exc()
            logger.error("%s failed. Cause %s. \nTrace: %s" % (task.type_name, e, trace))
            self.worker_fail(exception=e, trace=trace)

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
    def worker_success(self):
        self.get_task_collection().update_task(
            self._task,
            message="%s completed successfully!" % self._task.type_name)

        self.worker_finished(State.SUCCEEDED)

    ###########################################################################
    def worker_finished(self, state, message=None):

        # set end date
        self._task.end_date = date_now()
        self._task.state = state
        self.get_task_collection().update_task(
            self._task, properties=["state", "endDate", "nextRetryDate", "finalRetryDate"],
            event_name=EVENT_STATE_CHANGE, message=message)

        trigger_task_finished_event(self._task, state)

    ###########################################################################
    def worker_fail(self, exception, trace=None):
        if isinstance(exception, MBSError):
            log_msg = exception.message
        else:
            log_msg = "Unexpected error. Please contact admin"

        details = safe_stringify(exception)
        task = self._task

        self.get_task_collection().update_task(
            task, event_type=EventType.ERROR,
            message=log_msg, details=details, error_code=to_mbs_error_code(exception))

        # update retry info
        set_task_retry_info(task, exception)

        self.worker_finished(State.FAILED)

        # send a notification only if the task is not reschedulable
        # if there is an event queue configured then do not notify (because it should be handled by the backup
        # event listener)
        if not get_mbs().event_queue and task.exceeded_max_tries():
            get_mbs().notifications.notify_on_task_failure(task, exception, trace)


    ###########################################################################
    def get_worker_info(self):
        return {
            "pid": os.getpid()
        }



###############################################################################
# TaskCleanWorker
###############################################################################

class TaskCleanWorker(TaskWorker):

    ###########################################################################
    def __init__(self, task, env_vars=None):
        TaskWorker.__init__(self, task, env_vars=env_vars)

    ###########################################################################
    def get_cmd(self):
        return "clean-%s" % self._task.type_name.lower()

    ###########################################################################
    def run(self):
        try:
            self._task.cleanup()
        finally:
            self.cleaner_finished()

    ###########################################################################
    def cleaner_finished(self):
        self.worker_finished(State.CANCELED)

###############################################################################
def task_log_path(task):
    log_dir = task_log_dir(task.type_name.lower())
    log_file_name = "%s-%s.log" % (task.type_name.lower(), str(task.id))
    log_file_path = os.path.join(log_dir, log_file_name)

    return log_file_path

###############################################################################
def task_log_dir(task_type_name):
    log_dir = resolve_path(os.path.join(mbs_config.MBS_LOG_PATH, task_type_name.lower() + "s"))
    return log_dir

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


###############################################################################

LOG_FILE_ARCHIVE_CUTOFF_TIME = 24 * 2 * 60 * 60

LOG_FILE_DELETE_CUTOFF_TIME = 10 * LOG_FILE_ARCHIVE_CUTOFF_TIME

###############################################################################
# TaskLogFileSweeper
###############################################################################
class TaskLogFileSweeper(ScheduleRunner):

    ###############################################################################
    def __init__(self, task_type_name):
        super(TaskLogFileSweeper, self).__init__(schedule=Schedule(frequency_in_seconds=3600))
        self._logs_dir = task_log_dir(task_type_name)
        self._archive_logs_dir = os.path.join(self._logs_dir, "ARCHIVE")
        ensure_dir(self._logs_dir)
        ensure_dir(self._archive_logs_dir)

    ###############################################################################
    def tick(self):
        self._archive()
        self._sweep()

    ###############################################################################
    def _archive(self):
        cutoff_time = time.time() - LOG_FILE_ARCHIVE_CUTOFF_TIME
        for fname in os.listdir(self._logs_dir):
            try:
                fpath = os.path.join(self._logs_dir, fname)
                if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff_time:
                    self._archive_file(fpath)
            except Exception, ex:
                logger.exception("Error archiving log file '%s'" % fname)

    ###############################################################################
    def _archive_file(self, file_path):
        self._validate_log_file(file_path)

        file_name = os.path.basename(file_path)
        destination = os.path.join(self._archive_logs_dir, file_name)
        logger.debug("Archiving log file '%s'" % file_path)

        os.rename(file_path, destination)

    ###############################################################################
    def _sweep(self):
        cutoff_time = time.time() - LOG_FILE_DELETE_CUTOFF_TIME
        for fname in os.listdir(self._archive_logs_dir):
            try:
                fpath = os.path.join(self._archive_logs_dir, fname)
                if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff_time:
                    self._sweep_file(fpath)
            except Exception, ex:
                logger.exception("Error sweeping log file '%s'" % fname)

    ###############################################################################
    def _sweep_file(self, file_path):
        self._validate_log_file(file_path)
        logger.debug("Sweeping log file '%s'" % file_path)

        os.remove(file_path)

    ###############################################################################
    def _validate_log_file(self, file_path):

        if not (file_path.endswith(".log") and file_path.startswith(self._logs_dir)):
            raise Exception("file '%s' is not a valid log file" % file_path)


