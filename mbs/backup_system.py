__author__ = 'abdul'

import time
import urllib2
import traceback
import urllib
import json
import os

from threading import Thread, Timer


from utils import resolve_path, wait_for, get_validate_arg, dict_to_str, document_pretty_string

import mbs_config

from date_utils import date_now, date_minus_seconds, time_str_to_datetime_today
from errors import *
from auditors import GlobalAuditor
from globals import State, EventType

from task import EVENT_STATE_CHANGE

from mbs import get_mbs
from backup import Backup
from restore import Restore

from tags import DynamicTag

from plan import BackupPlan
from schedule import AbstractSchedule, Schedule
from retention import RetentionPolicy
from strategy import BackupStrategy
from target import BackupTarget
from source import BackupSource
from datetime import datetime

import persistence

from flask import Flask
from flask.globals import request
from monitor import BackupMonitor
from scheduler import BackupScheduler
from task_utils import set_task_retry_info, trigger_task_finished_event

from notification.handler import NotificationPriority, NotificationType
from schedule_runner import ScheduleRunner

###############################################################################
########################                                #######################
########################           Backup System        #######################
########################                                #######################
###############################################################################

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

###############################################################################
# Constants
###############################################################################

BACKUP_SYSTEM_STATUS_RUNNING = "running"
BACKUP_SYSTEM_STATUS_STOPPING = "stopping"
BACKUP_SYSTEM_STATUS_STOPPED = "stopped"

DEFAULT_BACKUP_SYSTEM_PORT = 8899
###############################################################################
# BackupSystem
###############################################################################
class BackupSystem(Thread):
    ###########################################################################
    def __init__(self, sleep_time=10):

        Thread.__init__(self)
        self._sleep_time = sleep_time

        self._plan_generators = []

        self._stop_requested = False
        self._stopped = False
        self._backup_expiration_manager = None
        self._backup_sweeper = None
        # auditing stuff

        # init global editor
        self._auditors = None
        self._global_auditor = None
        self._audit_schedule = None
        self._audit_next_occurrence = None

        self._port = DEFAULT_BACKUP_SYSTEM_PORT
        self._command_server = BackupSystemCommandServer(self)
        self._backup_monitor = BackupMonitor(self)
        self._scheduler = BackupScheduler(self)

        self._master_monitor = MbsMasterMonitor(self)

    ###########################################################################
    @property
    def plan_generators(self):
        return self._plan_generators

    @plan_generators.setter
    def plan_generators(self, value):
        self._plan_generators = value
        # set backup system to this
        if self._plan_generators:
            for pg in self._plan_generators:
                pg.backup_system = self

    ###########################################################################
    @property
    def auditors(self):
        return self._auditors

    @auditors.setter
    def auditors(self, value):
        self._auditors = value

    ###########################################################################
    @property
    def audit_schedule(self):
        return self._audit_schedule

    @audit_schedule.setter
    def audit_schedule(self, schedule):
        self._audit_schedule = schedule

    ###########################################################################
    @property
    def global_auditor(self):
        if not self._global_auditor:
            ac = get_mbs().audit_collection
            self._global_auditor = GlobalAuditor(audit_collection=ac)
            # register auditors with global auditor
            if self.auditors:
                for auditor in self.auditors:
                    self._global_auditor.register_auditor(auditor)

        return self._global_auditor

    ###########################################################################
    @property
    def backup_expiration_manager(self):
        return self._backup_expiration_manager

    @backup_expiration_manager.setter
    def backup_expiration_manager(self, val):
        self._backup_expiration_manager = val

    ####################################################################################################################
    @property
    def backup_sweeper(self):
        return self._backup_sweeper

    @backup_sweeper.setter
    def backup_sweeper(self, val):
        self._backup_sweeper = val

    ###########################################################################
    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, port):
        self._port = port

    ###########################################################################
    @property
    def backup_monitor(self):
        return self._backup_monitor

    ###########################################################################
    # Behaviors
    ###########################################################################
    def run(self):
        self.info("Starting up... ")
        self.info("PID is %s" % os.getpid())
        #self._update_pid_file()

        # Start the command server
        self._start_command_server()

        logger.info("Starting as Master instance")
        self.master_instance_run()
        self.master_instance_wait_for_stop_request()
        self.master_instance_stopped()


    ###########################################################################
    def master_instance_run(self):
        # ensure mbs indexes
        get_mbs().ensure_mbs_indexes()
        # Start expiration managers
        self._start_expiration_managers()

        # Start plan generators
        self._start_plan_generators()

        # start backup monitor
        self._start_backup_monitor()

        # start the scheduler
        self._start_scheduler()


        # start the master monitor
        self._start_master_monitor()

    ###########################################################################
    def master_instance_wait_for_stop_request(self):
        while not self._stop_requested:
            time.sleep(self._sleep_time)
        self.info('Stop request received by Master instance')

    ###########################################################################
    def master_instance_stopped(self):
        self.info('Stopping backup system Master threads...')
        self._stop_expiration_managers()
        self._stop_plan_generators()
        self._stop_backup_monitor()
        self._stop_scheduler()
        self._stop_master_monitor()
        self.info('All backup system Master threads stopped')
        self._stopped = True

    ###########################################################################
    def reschedule_all_failed_backups(self, force=False,
                                      reset_try_count=False):
        self.info("Rescheduling all failed backups")

        q = {
            "state": State.FAILED
        }

        for backup in get_mbs().backup_collection.find_iter(q):
            try:
                self.reschedule_backup(backup, force=force,
                                       reset_try_count=reset_try_count)
            except Exception, e:
                logger.error(e)

    ###########################################################################
    def reschedule_backup(self, backup, force=False,
                          reset_try_count=False):
        """
            Reschedules the backup IF backup state is FAILED and
                        backup is still within it's plan current cycle
        """
        if backup.state != State.FAILED:
            msg = ("Cannot reschedule backup ('%s', '%s'). Rescheduling is "
                   "only allowed for backups whose state is '%s'." %
                   (backup.id, backup.state, State.FAILED))
            raise BackupSystemError(msg)
        elif backup.plan and backup.plan.next_occurrence <= date_now():
            msg = ("Cannot reschedule backup '%s' because its occurrence is"
                   " in the past of the current cycle" % backup.id)
            raise BackupSystemError(msg)

        self.info("Rescheduling backup %s" % backup._id)
        props = ["state", "tags", "nextRetryDate"]
        backup.state = State.SCHEDULED
        # clear out next retry date
        backup.next_retry_date = None

        bc = get_mbs().backup_collection
        # if force is set then clear backup log
        if force:
            backup.logs = []
            backup.try_count = 0
            backup.engine_guid = None
            props.extend(["logs", "tryCount", "engineGuid"])

        if reset_try_count or force:
            backup.try_count = 0
            props.append("tryCount")
        # regenerate backup tags if backup belongs to a plan
        if backup.plan and backup.plan.tags:
            backup.tags = backup.plan.tags.copy()

        try:
            self._resolve_task_tags(backup)
        except Exception, ex:
            self._task_failed_to_schedule(backup, bc, ex)

        if backup.state == State.FAILED:
            if not get_mbs().event_queue:
                get_mbs().notifications.notify_task_reschedule_failed(backup)
            trigger_task_finished_event(backup, State.FAILED)

        bc.update_task(backup, properties=props,
                       event_name=EVENT_STATE_CHANGE,
                       message="Rescheduling")


    ###########################################################################
    def reschedule_restore(self, restore, force=False):
        """
            Reschedules the restore IF state is FAILED
        """
        if restore.state != State.FAILED:
            msg = ("Cannot reschedule restore ('%s', '%s'). Rescheduling is "
                   "only allowed for restores whose state is '%s'." %
                   (restore.id, restore.state, State.FAILED))
            raise BackupSystemError(msg)

        self.info("Rescheduling restore %s" % restore.id)
        props = ["state", "tags"]
        restore.state = State.SCHEDULED

        rc = get_mbs().restore_collection
        # if force is set then clear restore log
        if force:
            restore.logs = []
            restore.try_count = 0
            restore.engine_guid = None
            props.extend(["logs", "tryCount", "engineGuid"])


        rc.update_task(restore, properties=props,
                       event_name=EVENT_STATE_CHANGE,
                       message="Rescheduling")
    ###########################################################################
    def schedule_plan_backup(self, plan, one_time=False):
        self.info("Scheduling plan '%s'" % plan.id)

        plan_occurrence = None
        backup_plan = None

        if not one_time:
            backup_plan = plan
            plan_occurrence = plan.next_occurrence
            plan.next_occurrence = plan.schedule.next_natural_occurrence()

        # create a copy of plan tags to backup to keep original plan tag values
        tags = plan.tags.copy() if plan.tags else None
        # TODO XXX temporarily setting max allowed lag here just incase some engine has old bug of computing max lag

        strategy = plan.strategy
        if not strategy.max_lag_seconds and plan_occurrence:
            strategy.max_lag_seconds = plan.schedule.max_acceptable_lag(plan_occurrence)

        backup = self.schedule_backup(strategy=plan.strategy,
                                      source=plan.source,
                                      target=plan.target,
                                      priority=plan.priority,
                                      tags=tags,
                                      plan_occurrence=plan_occurrence,
                                      plan=backup_plan,
                                      secondary_targets=plan.secondary_targets)

        #  update the plans next occurrence
        self._save_plan_next_occurrence(plan)

        self._request_plan_retention(plan)

        return backup

    ###########################################################################
    def schedule_backup(self, **kwargs):

        try:
            backup = Backup()
            backup.created_date = date_now()
            backup.strategy = get_validate_arg(kwargs, "strategy",
                                               expected_type=BackupStrategy)
            backup.source = get_validate_arg(kwargs, "source", BackupSource)
            backup.target = get_validate_arg(kwargs, "target", BackupTarget)
            backup.priority = get_validate_arg(kwargs, "priority",
                                               expected_type=(int, long,
                                                              float, complex),
                                               required=False)
            backup.plan_occurrence = \
                get_validate_arg(kwargs, "plan_occurrence",
                                 expected_type=datetime,
                                 required=False)
            backup.plan = get_validate_arg(kwargs, "plan",
                                           expected_type=BackupPlan,
                                           required=False)

            backup.secondary_targets = get_validate_arg(kwargs,
                                                        "secondary_targets",
                                                        expected_type=list,
                                                        required=False)

            backup.change_state(State.SCHEDULED)
            # set tags
            tags = get_validate_arg(kwargs, "tags", expected_type=dict,
                                    required=False)

            backup.tags = tags

            bc = get_mbs().backup_collection
            try:
                # resolve tags

                self._resolve_task_tags(backup)
            except Exception, ex:
                self._task_failed_to_schedule(backup, bc, ex)

            backup_doc = backup.to_document()
            get_mbs().backup_collection.save_document(backup_doc)
            # set the backup id from the saved doc

            backup.id = backup_doc["_id"]

            self.info("Saved backup \n%s" % backup)

            if backup.state == State.FAILED:
                trigger_task_finished_event(backup, State.FAILED)

            return backup
        except Exception, e:
            args_str = dict_to_str(kwargs)
            msg = ("Failed to schedule backup. Args:\n %s" % args_str)
            logger.error(msg)
            logger.error(traceback.format_exc())
            raise BackupSchedulingError(msg=msg, cause=e)


    ###########################################################################
    def create_backup_plan(self, **kwargs):
        try:
            plan = BackupPlan()
            plan.created_date = date_now()

            plan.description = get_validate_arg(kwargs, "description",
                                             expected_type=(str, unicode),
                                             required=False)

            plan.strategy = get_validate_arg(kwargs, "strategy",
                                             expected_type=BackupStrategy)


            plan.schedule = get_validate_arg(kwargs, "schedule",
                                             expected_type=AbstractSchedule)

            plan.source = get_validate_arg(kwargs, "source",
                                           expected_type=BackupSource)

            plan.target = get_validate_arg(kwargs, "target",
                                           expected_type=BackupTarget)

            plan.retention_policy = get_validate_arg(kwargs, "retention_policy",
                                                     expected_type=
                                                     RetentionPolicy,
                                                     required=False)

            plan.priority = get_validate_arg(kwargs, "priority",
                                             expected_type=(int, long,
                                                            float, complex),
                                             required=False)

            plan.secondary_targets = get_validate_arg(kwargs,
                                                      "secondary_targets",
                                                      expected_type=list,
                                                      required=False)

            # tags
            plan.tags = get_validate_arg(kwargs, "tags", expected_type=dict,
                                         required=False)

            plan_doc = plan.to_document()
            get_mbs().plan_collection.save_document(plan_doc)
            # set the backup plan id from the saved doc

            plan.id = plan_doc["_id"]

            self.info("Saved backup plan \n%s" % plan)
            # process plan to set next occurrence
            self._scheduler._process_plan(plan)
            return plan
        except Exception, e:
            args_str = dict_to_str(kwargs)
            msg = ("Failed to create plan. Args:\n %s" % args_str)
            logger.error(msg)
            logger.error(traceback.format_exc())
            raise CreatePlanError(msg=msg, cause=e)

    ###########################################################################
    def _set_update_plan_next_occurrence(self, plan):
        plan.next_occurrence = plan.schedule.next_natural_occurrence()
        self._save_plan_next_occurrence(plan)

    ###########################################################################
    def _save_plan_next_occurrence(self, plan):
        q = {"_id": plan.id}
        u = {
            "$set": {
                "nextOccurrence": plan.next_occurrence
            }
        }
        get_mbs().plan_collection.update(spec=q, document=u)

    ###########################################################################
    def save_plan(self, plan):
        try:

            self.debug("Validating plan %s" % plan)
            errors = plan.validate()
            if errors:
                err_msg = ("Plan %s is invalid."
                           "Please correct the following errors and then try"
                           " saving again.\n%s" % (plan, errors))

                raise BackupSystemError(err_msg)

            # set plan created date if its not set
            if not plan.created_date:
                plan.created_date = date_now()

            is_new_plan = not plan.id

            if is_new_plan:
                self.info("Saving new plan: \n%s" % plan)
                plan_doc = plan.to_document()
                get_mbs().plan_collection.save_document(plan_doc)
                plan.id = plan_doc["_id"]
                self.info("Plan saved successfully")
            else:
                self.info("Updating plan: \n%s" % plan)
                self.update_existing_plan(plan)
                self.info("Plan updated successfully")


        except Exception, e:
            raise BackupSystemError("Error while saving plan %s. %s" %
                                       (plan, e))

    ###########################################################################
    def update_existing_plan(self, plan):
        # TODO XXX remove the pymongo collection save call because it is deprecated
        return get_mbs().plan_collection.collection.save(plan.to_document())

    ###########################################################################
    def remove_plan(self, plan_id):
        plan = get_mbs().plan_collection.get_by_id(plan_id)
        if plan:
            plan.deleted_date = date_now()
            logger.info("Adding plan '%s' to deleted plans" % plan_id)
            get_mbs().deleted_plan_collection.save_document(plan.to_document())
            logger.info("Removing plan '%s' from plans" % plan_id)
            get_mbs().plan_collection.remove_by_id(plan_id)
        else:
            logger.info("No such plan '%s'" % plan_id)

    ###########################################################################
    def _request_plan_retention(self, plan):
        # request a plan retention
        if self.backup_expiration_manager:
            self.backup_expiration_manager.request_plan_retention(plan)

    ###########################################################################
    def get_backup_database_names(self, backup_id):
        """
            Returns the list of databases available by specified backup
        """
        backup = persistence.get_backup(backup_id)

        if backup and backup.source_stats:
            if "databaseName" in backup.source_stats:
                return [backup.source_stats["databaseName"]]
            elif "databaseStats" in backup.source_stats:
                return backup.source_stats["databaseStats"].keys()

    ###########################################################################
    def schedule_backup_restore(self, backup_id, destination_uri,tags=None,
                                no_index_restore=None, no_users_restore=None, no_roles_restore=None,
                                source_database_name=None):
        backup = get_mbs().backup_collection.get_by_id(backup_id)
        destination = build_backup_source(destination_uri)
        logger.info("Scheduling a restore for backup '%s'" % backup.id)
        restore = Restore()

        restore.state = State.SCHEDULED
        restore.source_backup = backup
        restore.source_database_name = source_database_name
        restore.strategy = backup.strategy
        restore.strategy.no_index_restore = no_index_restore
        restore.strategy.no_users_restore = no_users_restore
        restore.strategy.no_roles_restore = no_roles_restore
        restore.destination = destination
        # resolve tags
        tags = tags or restore.source_backup.tags
        restore.tags = tags

        rc = get_mbs().restore_collection
        try:
            self._resolve_task_tags(restore)
        except Exception, ex:
            self._task_failed_to_schedule(restore, rc, ex)

        restore.created_date = date_now()

        logger.info("Saving restore task: %s" % restore)
        restore_doc = restore.to_document()
        rc.save_document(restore_doc)
        restore.id = restore_doc["_id"]

        return restore

    ###########################################################################
    def get_current_restore_by_destination(self, destination_uri):
        destination = build_backup_source(destination_uri)

        q = {"destination.%s" % key: value for (key, value)
             in destination.to_document().items()}

        q.update({
            "state": {
                "$nin": [
                    State.SUCCEEDED
                ]
            }
        })
        sort = [("createdDate", -1)]
        restore = get_mbs().restore_collection.find_one(q, sort=sort)

        return restore

    ###########################################################################
    def _check_audit(self):
        # TODO Properly run auditors as needed

        #
        if not self._audit_next_occurrence:
            self._audit_next_occurrence = self._get_audit_next_occurrence()
            return

        if date_now() >= self._audit_next_occurrence():
            self.info("Running auditor...")
            self.global_auditor.generate_yesterday_audit_reports()
            self._audit_next_occurrence = self._get_audit_next_occurrence()

    ###########################################################################
    def _get_audit_next_occurrence(self):
        pass

    ###########################################################################
    def _audit_date_for_today(self):
        if self._audit_schedule:
            return time_str_to_datetime_today(self._audit_schedule)

    ###########################################################################
    def _audit_date_for_today(self):
        if self._audit_schedule:
            return time_str_to_datetime_today(self._audit_schedule)

    ###########################################################################
    def _resolve_task_tags(self, task):
        if task.tags:
            for name, value in task.tags.items():
                if isinstance(value, DynamicTag):
                    task.tags[name] = value.generate_tag_value(task)


    ####################################################################################################################
    def _task_failed_to_schedule(self, task, task_collection, exception):

        # log error
        msg = ("Failed to schedule task. Trace: \n%s" %
               traceback.format_exc())
        logger.error(msg)
        logger.error(traceback.format_exc())
        # set state to failed
        task.state = State.FAILED
        # bump up try count
        task.try_count += 1

        set_task_retry_info(task, exception)

        error_code = to_mbs_error_code(exception)
        if not task.id:
            task.log_event(event_type=EventType.ERROR, message=msg, error_code=error_code)
        else:
            tc = task_collection
            tc.update_task(task,
                           properties=["state", "tryCount", "nextRetryDate", "finalRetryDate"],
                           event_name="FAILED_TO_SCHEDULE",
                           details=msg,
                           error_code=error_code,
                           event_type=EventType.ERROR)


    ###########################################################################
    def _kill_backup_system_process(self):
        self.info("Attempting to kill backup system process")
        pid = self._read_process_pid()
        if pid:
            self.info("Killing backup system process '%s' using signal 9" %
                      pid)
            os.kill(int(pid), 9)
        else:
            raise BackupSystemError("Unable to determine backup system process"
                                    " id")

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
        pid_file_name = "backup_system_pid.txt"
        return resolve_path(os.path.join(mbs_config.mbs_conf_dir(),
                                         pid_file_name))

    ###########################################################################
    # BackupSystem stopping
    ###########################################################################
    def force_stop(self):
        """
            Sends a stop request to the backup system using the api port
            This should be used by other processes (copy of the backup system
            instance) but not the actual running backup system process
        """
        self._kill_backup_system_process()
        return


    ###########################################################################
    def get_status(self):
        """
            Sends a status request to the backup system using the api port
            This should be used by other processes (copy of the backup system
            instance) but not the actual running backup system process
        """
        url = "http://0.0.0.0:%s/status" % self.port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                return json.loads(response.read().strip())
            else:
                msg = ("Error while trying to get status backup system URL %s"
                       " (Response code %s)" % (url, response.getcode()))
                raise BackupSystemError(msg)

        except IOError, ioe:
            return {
                "status": BACKUP_SYSTEM_STATUS_STOPPED
            }


    ###########################################################################
    def stop_backup_system(self):
        """
            Sends a status request to the backup system using the api port
            This should be used by other processes (copy of the backup system
            instance) but not the actual running backup system process
        """
        url = "http://0.0.0.0:%s/stop" % self.port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                return json.loads(response.read().strip())
            else:
                msg = ("Error while trying to stop backup system URL %s"
                       " (Response code %s)" % (url, response.getcode()))
                raise BackupSystemError(msg)

        except IOError, ioe:
            return {
                "status": BACKUP_SYSTEM_STATUS_STOPPED
            }

    ###########################################################################
    def request_stop(self):
        """
            Triggers the backup system to gracefully stop
        """
        Timer(1, self._do_stop).start()

    ###########################################################################
    def _do_stop(self):
        """
            Triggers the backup system to gracefully stop
        """
        self.info("Stopping backup system gracefully")

        self._stop_requested = True
        # wait until backup system stops

        def stopped():
            return self._stopped

        self.info("Waiting for backup system to stop")
        wait_for(stopped, timeout=60)
        if stopped():
            self.info("Backup system stopped successfully. Bye!")

        else:
            self.error("Backup system did not stop in 60 seconds")


    ###########################################################################
    def do_get_status(self):
        """
            Gets the status of the backup system
        """
        if self._stopped:
            status = BACKUP_SYSTEM_STATUS_STOPPED
        elif self._stop_requested:
            status = BACKUP_SYSTEM_STATUS_STOPPING
        else:
            status = BACKUP_SYSTEM_STATUS_RUNNING

        return {
            "status": status,
            "versionInfo": get_mbs().get_version_info()
        }

    ###########################################################################
    # Backup expiration manager
    ###########################################################################

    def _start_expiration_managers(self):
        if self.backup_expiration_manager:
            self.info("Starting Backup Expiration Manager")
            self.backup_expiration_manager.start()
        if self.backup_sweeper:
            self.info("Starting Backup Sweeper")
            self.backup_sweeper.start()

    ###########################################################################
    def _stop_expiration_managers(self):
        if self.backup_expiration_manager:
            self.info("Stopping Backup Expiration Manager")
            self.backup_expiration_manager.stop()
            self.info("Backup Expiration Manager stopped!")
        if self.backup_sweeper:
            self.info("Stopping Backup Sweeper")
            self.backup_sweeper.stop()
            self.info("Backup Sweeper stopped!")

    ###########################################################################
    # Backup plan generators
    ###########################################################################

    ###########################################################################
    def _start_plan_generators(self):
        self.info('Starting Plan Generators')
        if self.plan_generators:
            for pg in self.plan_generators:
                pg.start()


    ###########################################################################
    def _stop_plan_generators(self):
        self.info('Stopping Plan Generators')
        if self.plan_generators:
            for pg in self.plan_generators:
                pg.stop()
        self.info('Plan Generators stopped!')

    ###########################################################################
    # Backup monitor
    ###########################################################################

    ###########################################################################
    def _start_backup_monitor(self):
        self.info('Starting Backup Monitor')
        self._backup_monitor.start()

    ###########################################################################
    def _stop_backup_monitor(self):
        self.info('Stopping Backup Monitor')
        self._backup_monitor.stop()
        self.info('Backup Monitor stopped!')

    ###########################################################################
    # Backup scheduler
    ###########################################################################

    ###########################################################################
    def _start_scheduler(self):
        self.info('Starting Scheduler')
        self._scheduler.start()

    ###########################################################################
    def _stop_scheduler(self):
        self.info('Stopping Scheduler')
        self._scheduler.stop()
        self.info('Scheduler stopped!')

    ###########################################################################
    def _start_master_monitor(self):
        self.info('Starting Master Monitor')
        self._master_monitor.start()

    ###########################################################################
    def _stop_master_monitor(self):
        self.info('Stopping Master Monitor')
        self._master_monitor.stop()
        self.info('Master Monitor stopped!')

    ###########################################################################
    def monitor_master(self):
        services_down = []
        if not self._scheduler.is_alive():
            services_down.append("Scheduler")
        if self._backup_expiration_manager and not self._backup_expiration_manager.is_alive():
            services_down.append("Expiration Manager")

        if self._backup_sweeper and not self._backup_sweeper.is_alive():
            services_down.append("Backup Sweeper")

        if self._plan_generators:
            for g in self._plan_generators:
                if not g.is_alive():
                    services_down.append("Plan Generator: '%s'" % g.name)

        if services_down:
            msg = "Mbs Master has some services down: %s" % "\n".join(services_down)
            logger.error(msg)
            get_mbs().notifications.send_event_notification("Master Services DOWN!!!!",
                                                            msg, priority=NotificationPriority.CRITICAL)


    ###########################################################################
    # Command Server
    ###########################################################################

    def _start_command_server(self):
        self.info("Starting command server at port %s" % self.port)

        self._command_server.start()
        self.info("Command Server started successfully!")

    ###########################################################################
    def _stop_command_server(self):
        self.info("Stopping command server")
        self._command_server.stop()

    ###########################################################################
    # logging
    ###########################################################################
    def info(self, msg):
        logger.info("BackupSystem: %s" % msg)

    ###########################################################################
    def error(self, msg):
        logger.error("BackupSystem: %s" % msg)

    ###########################################################################
    def debug(self, msg):
        logger.debug("BackupSystem: %s" % msg)

###############################################################################

def build_backup_source(uri):
    """
        Builds a backup source of the specified URI
    """
    return get_mbs().backup_source_builder.build_backup_source(uri)




###############################################################################
# BackupSystemCommandServer
###############################################################################
class BackupSystemCommandServer(Thread):

    ###########################################################################
    def __init__(self, backup_system):
        Thread.__init__(self)
        self.daemon = True
        self._backup_system = backup_system
        self._flask_server = self._build_flask_server()

    ###########################################################################
    def _build_flask_server(self):
        flask_server = Flask(__name__)
        backup_system = self._backup_system
        ## build stop method
        @flask_server.route('/stop', methods=['GET'])
        def stop_backup_system():
            logger.info("Command Server: Received a stop command")
            try:
                # stop the backup system
                backup_system.request_stop()
                return document_pretty_string({
                    "ok": True
                })
            except Exception, e:
                msg = "Error while trying to stop backup system: %s" % e
                logger.error(msg)
                logger.error(traceback.format_exc())
                return document_pretty_string({"error": "can't stop"})

        ## build status method
        @flask_server.route('/status', methods=['GET'])
        def status():
            logger.info("Command Server: Received a status command")
            try:
                return document_pretty_string(backup_system.do_get_status())
            except Exception, e:
                msg = "Error while trying to get backup system status: %s" % e
                logger.error(msg)
                logger.error(traceback.format_exc())
                return {
                    "status": "UNKNOWN",
                    "error": msg
                }

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
        logger.info("BackupSystemCommandServer: Running flask server ")
        self._flask_server.run(host="0.0.0.0", port=self._backup_system.port,
                               threaded=True)

    ###########################################################################
    def stop(self):

        logger.info("BackupSystemCommandServer: Stopping flask server ")
        port = self._backup_system.port
        url = "http://0.0.0.0:%s/stop-command-server" % port
        try:
            response = urllib2.urlopen(url, timeout=30)
            if response.getcode() == 200:
                logger.info("BackupSystemCommandServer: Flask server stopped "
                            "successfully")
                return response.read().strip()
            else:
                msg = ("Error while trying to get backup system  URL %s "
                       "(Response code %s)" % (url,response.getcode()))
                raise BackupSystemError(msg)

        except Exception, e:
            raise BackupSystemError("Error while stopping flask server:"
                                    " %s" % e)

#################################################################################
class MbsMasterMonitor(ScheduleRunner):

    ###########################################################################
    def __init__(self, master):
        self._master = master
        super(MbsMasterMonitor, self).__init__(Schedule(frequency_in_seconds=10))

    ###########################################################################
    def tick(self):
        try:
            self._master.monitor_master()
        except Exception, ex:
            logger.exception("MbsMasterMonitor error")
            get_mbs().notifications.send_event_notification("MbsMasterMonitor error",
                                                            str(ex), priority=NotificationPriority.CRITICAL)

