__author__ = 'abdul'

import time
import mbs_logging
import traceback
import urllib
import json
import os

from threading import Thread


from utils import resolve_path, wait_for, get_validate_arg, dict_to_str

import mbs_config

from date_utils import date_now, date_minus_seconds, time_str_to_datetime_today
from errors import *
from auditors import GlobalAuditor
from task import (STATE_SCHEDULED, STATE_IN_PROGRESS, STATE_FAILED,
                  STATE_CANCELED, STATE_SUCCEEDED, EVENT_STATE_CHANGE)

from mbs import get_mbs
from api import BackupSystemApiServer
from backup import Backup
from restore import Restore

from tags import DynamicTag

from plan import BackupPlan
from strategy import BackupStrategy
from target import BackupTarget
from source import BackupSource
from datetime import datetime

import persistence
import retention


###############################################################################
########################                                #######################
########################           Backup System        #######################
########################                                #######################
###############################################################################

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
# Constants
###############################################################################
# maximum backup wait time in seconds: default to five hours
MAX_BACKUP_WAIT_TIME = 5 * 60 * 60
ONE_OFF_BACKUP_MAX_WAIT_TIME = 60

# Minimum time before rescheduling a failed backup (5 minutes)
RESCHEDULE_PERIOD = 5 * 60
RESCHEDULE_PERIOD_MILLS = RESCHEDULE_PERIOD * 1000

BACKUP_SYSTEM_STATUS_RUNNING = "running"
BACKUP_SYSTEM_STATUS_STOPPING = "stopping"
BACKUP_SYSTEM_STATUS_STOPPED = "stopped"

###############################################################################
# BackupSystem
###############################################################################
class BackupSystem(Thread):
    ###########################################################################
    def __init__(self, sleep_time=10,
                 api_port=9003):

        Thread.__init__(self)
        self._sleep_time = sleep_time

        self._plan_generators = []
        self._tick_count = 0
        self._stop_requested = False
        self._stopped = False
        self._api_port = api_port
        self._api_server = None
        self._backup_sweeper = None
        # auditing stuff

        # init global editor
        self._audit_notification_handler = None
        self._auditors = None
        self._global_auditor = None
        self._audit_schedule = None
        self._audit_next_occurrence = None

    ###########################################################################
    @property
    def plan_generators(self):
        return self._plan_generators

    @plan_generators.setter
    def plan_generators(self, value):
        self._plan_generators = value

    ###########################################################################
    @property
    def auditors(self):
        return self._auditors

    @auditors.setter
    def auditors(self, value):
        self._auditors = value

    ###########################################################################
    @property
    def audit_notification_handler(self):
        return self._audit_notification_handler

    @audit_notification_handler.setter
    def audit_notification_handler(self, handler):
        self._audit_notification_handler = handler

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
            nh = self.audit_notification_handler
            self._global_auditor = GlobalAuditor(audit_collection=ac,
                                                 notification_handler=nh)
            # register auditors with global auditor
            if self.auditors:
                for auditor in self.auditors:
                    self._global_auditor.register_auditor(auditor)

        return self._global_auditor

    ###########################################################################
    @property
    def api_server(self):
        if not self._api_server:
            self._api_server = BackupSystemApiServer()

        return self._api_server

    @api_server.setter
    def api_server(self, api_server):
        self._api_server = api_server
        self._api_server._backup_system = self

    ###########################################################################
    @property
    def backup_sweeper(self):
        if not self._backup_sweeper:
            self._backup_sweeper = retention.BackupSweeper()

        return self._backup_sweeper

    @backup_sweeper.setter
    def backup_sweeper(self, backup_sweeper):
        self._backup_sweeper = backup_sweeper

    ###########################################################################
    # Behaviors
    ###########################################################################
    def run(self):
        self.info("Starting up... ")
        self.info("PID is %s" % os.getpid())
        self._update_pid_file()

        # Start the api server
        self._start_api_server()

        # Start the sweeper
        self._start_backup_sweeper()

        while not self._stop_requested:
            try:
                self._tick()
                time.sleep(self._sleep_time)
            except Exception, e:
                self.error("Caught an error: '%s'.\nStack Trace:\n%s" %
                           (e, traceback.format_exc()))
                self._notify_error(e)

        self._stop_backup_sweeper()
        self._stopped = True

    ###########################################################################
    def _tick(self):

        # increase _generators_tick_counter
        self._tick_count += 1

        self._process_plans_considered_now()

        # run those things every 100 ticks
        if self._tick_count % 100 == 0:
            self._notify_on_past_due_scheduled_backups()
            self._notify_on_late_in_progress_backups()
            self._cancel_past_cycle_scheduled_backups()
            self._run_plan_generators()
            self._reschedule_in_cycle_failed_backups()

    ###########################################################################
    def _process_plans_considered_now(self):
        for plan in self._get_plans_to_consider_now():
            try:
                self._process_plan(plan)
            except Exception, e:
                logger.error("Error while processing plan '%s'. Cause: %s" %
                             (plan.id, e))
                self._notify_error(e)

    ###########################################################################
    def _process_plan(self, plan):
        """
        Schedule the plan if the following conditions apply
        """
        self.info("Processing plan '%s'" % plan._id)
        # validate plan first
        self.debug("Validating plan '%s'" % plan._id)

        errors = plan.validate()
        if errors:
            err_msg = ("Plan '%s' is invalid.Please correct the following"
                       " errors.\n%s" % (plan.id, errors))
            raise InvalidPlanError(err_msg)
            # TODO disable plan ???

        now = date_now()
        next_natural_occurrence = plan.next_natural_occurrence()


        # CASE I: First time <==> No previous backups
        # Only set the next occurrence here
        if not plan.next_occurrence:
            self.info("Plan '%s' has no previous backup. Setting next"
                      " occurrence to '%s'" %
                      (plan._id, next_natural_occurrence))

            self._set_update_plan_next_occurrence(plan)

        # CASE II: If there is a backup running (IN PROGRESS)
        # ===> no op
        elif self._plan_has_backup_in_progress(plan):
            self.info("Plan '%s' has a backup that is currently in"
                      " progress. Nothing to do now." % plan._id)
        # CASE III: if time now is past the next occurrence
        elif plan.next_occurrence <= now:
            self.info("Plan '%s' next occurrence '%s' is greater than"
                      " now. Scheduling a backup!!!" %
                      (plan._id, plan.next_occurrence))

            self.schedule_plan_backup(plan)


        else:
            self.info("Wooow. How did you get here!!!! Plan '%s' does"
                      " not to be scheduled yet. next natural "
                      "occurrence %s " % (plan._id,
                                          next_natural_occurrence))


    ###########################################################################
    def _get_plans_to_consider_now(self):
        """
        Returns list of plans that the scheduler should process at this time.
        Those are:
            1- Plans with no backups scheduled yet (next occurrence has not
            been calculated yet)

            2- Plans whose next occurrence is now or in the past

        """
        now = date_now()
        q = {"$or": [
                {"nextOccurrence": {"$exists": False}},
                {"nextOccurrence": None},
                {"nextOccurrence": {"$lte": now}}
            ]
        }


        return get_mbs().plan_collection.find(q)

    ###########################################################################
    def _plan_has_backup_in_progress(self, plan):
        q = {
            "plan.$id": plan._id,
            "state": STATE_IN_PROGRESS
        }
        return get_mbs().backup_collection.find_one(q) is not None

    ###########################################################################
    def _cancel_past_cycle_scheduled_backups(self):
        """
        Cancels scheduled backups whose plan's next occurrence in in the past
        """
        now = date_now()

        q = {
            "state": {"$in": [STATE_SCHEDULED]},
            "plan.nextOccurrence": {"$lte": now}
        }

        bc = get_mbs().backup_collection
        for backup in bc.find(q):
            self.info("Cancelling backup %s" % backup._id)
            backup.state = STATE_CANCELED
            bc.update_task(backup, properties="state",
                           event_name=EVENT_STATE_CHANGE,
                           message="Backup is past due. Canceling...")

    ###########################################################################
    def _reschedule_in_cycle_failed_backups(self):
        """
        Reschedule failed reschedulable backups that failed at least
        RESCHEDULE_PERIOD seconds ago
        """

        # select backups whose last log date is at least RESCHEDULE_PERIOD ago

        where = ("(this.logs[this.logs.length-1].date.getTime() + %s) < "
                 "new Date().getTime()" % RESCHEDULE_PERIOD_MILLS)
        q = {
            "state": STATE_FAILED,
            "reschedulable": True,
            "$where": where
        }

        for backup in get_mbs().backup_collection.find(q):
            self.reschedule_backup(backup)

    ###########################################################################
    def reschedule_all_failed_backups(self, from_scratch=False):
        self.info("Rescheduling all failed backups")

        q = {
            "state": STATE_FAILED
        }

        for backup in get_mbs().backup_collection.find(q):
            try:
                self.reschedule_backup(backup, from_scratch=from_scratch)
            except Exception, e:
                logger.error(e)

    ###########################################################################
    def reschedule_backup(self, backup, from_scratch=False):
        """
            Reschedules the backup IF backup state is FAILED and
                        backup is still within it's plan current cycle
        """
        if backup.state != STATE_FAILED:
            msg = ("Cannot reschedule backup ('%s', '%s'). Rescheduling is "
                   "only allowed for backups whose state is '%s'." %
                   (backup.id, backup.state, STATE_FAILED))
            raise BackupSystemError(msg)
        elif backup.plan and backup.plan.next_occurrence <= date_now():
            msg = ("Cannot reschedule backup '%s' because its occurrence is"
                   " in the past of the current cycle" % backup.id)
            raise BackupSystemError(msg)

        self.info("Rescheduling backup %s" % backup._id)
        backup.state = STATE_SCHEDULED
        # regenerate backup tags if backup belongs to a plan
        if backup.plan:
            backup.tags = self._resolve_task_tags(backup, backup.plan.tags)

        bc = get_mbs().backup_collection
        # if from_scratch is set then clear backup log
        if from_scratch:
            backup.logs = []
            backup.try_count = 0
            backup.engine_guid = None
            bc.update_task(backup, properties=["logs", "tryCount",
                                               "engineGuid"])

        bc.update_task(backup, properties=["state", "tags"],
                       event_name=EVENT_STATE_CHANGE,
                       message="Rescheduling")

    ###########################################################################
    def schedule_plan_backup(self, plan, one_time=False):
        self.info("Scheduling plan '%s'" % plan._id)

        plan_occurrence = None
        backup_plan = None

        if not one_time:
            backup_plan = plan
            plan_occurrence = plan.next_occurrence
            plan.next_occurrence = plan.next_natural_occurrence()

        backup = self.schedule_backup(strategy=plan.strategy,
                                      source=plan.source,
                                      target=plan.target,
                                      priority=plan.priority,
                                      tags=plan.tags,
                                      plan_occurrence=plan_occurrence,
                                      plan=backup_plan)

        #  update the plans next occurrence
        self._save_plan_next_occurrence(plan)

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
            backup.change_state(STATE_SCHEDULED)
            # resolve tags
            tags = get_validate_arg(kwargs, "tags", expected_type=dict,
                                    required=False)
            backup.tags = self._resolve_task_tags(backup, tags)

            backup_doc = backup.to_document()
            get_mbs().backup_collection.save_document(backup_doc)
            # set the backup id from the saved doc

            backup.id = backup_doc["_id"]

            self.info("Scheduled backup \n%s" % backup)
            return backup
        except Exception, e:
            args_str = dict_to_str(kwargs)
            msg = ("Failed to schedule backup. Args:\n %s" % args_str)
            logger.error(msg)
            logger.error(traceback.format_exc())
            raise BackupSchedulingError(msg=msg, cause=e)

    ###########################################################################
    def _set_update_plan_next_occurrence(self, plan):
        plan.next_occurrence = plan.next_natural_occurrence()
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

            if plan.id:
                self.info("Updating plan: \n%s" % plan)
            else:
                self.info("Saving new plan: \n%s" % plan)

            get_mbs().plan_collection.save_document(plan.to_document())

            self.info("Plan saved successfully")
        except Exception, e:
            raise BackupSystemError("Error while saving plan %s. %s" %
                                       (plan, e))

    ###########################################################################
    def remove_plan(self, plan):
        logger.info("Removing plan '%s' " % plan.id)
        get_mbs().plan_collection.remove_by_id(plan.id)

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
    def delete_backup(self, backup_id):
        """
            Deletes the specified backup. Deleting here means expiring
        """
        backup = persistence.get_backup(backup_id)
        if (backup and backup.state == STATE_SUCCEEDED and
            not backup.expired_date):
            retention.expire_backup(backup)
            return True

        return False

    ###########################################################################
    def schedule_backup_restore(self, backup_id, destination_uri,
                                tags=None,
                                source_database_name=None):
        backup = get_mbs().backup_collection.get_by_id(backup_id)
        destination = build_backup_source(destination_uri)
        logger.info("Scheduling a restore for backup '%s'" % backup.id)
        restore = Restore()

        restore.source_backup = backup
        restore.source_database_name = source_database_name
        restore.strategy = backup.strategy
        restore.destination = destination
        # resolve tags
        tags = tags or restore.source_backup.tags
        restore.tags = self._resolve_task_tags(restore, tags)

        restore.state = STATE_SCHEDULED
        restore.created_date = date_now()

        logger.info("Saving restore task: %s" % restore)
        restore_doc = restore.to_document()
        get_mbs().restore_collection.save_document(restore_doc)
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
                    STATE_SUCCEEDED
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
    # plan generators methods
    ###########################################################################
    def _run_plan_generators(self):
        self.info("Running ALL plan generators")
        for generator in self.plan_generators:
                self._run_generator(generator)

    ###########################################################################
    def _run_generator(self, generator):
        self.info("Running plan generator '%s' " % generator.name)
        # remove expired plans
        for plan in generator.get_plans_to_remove():
            self.remove_plan(plan)

        # save new plans
        for plan in generator.get_plans_to_save():
            self.save_plan(plan)

    ###########################################################################
    def _notify_on_past_due_scheduled_backups(self):
        """
            Send notifications for jobs that has been scheduled for a period
            longer than min(half the frequency, 5 hours) of its plan.
             If backup does not have a plan (i.e. one off)
             then it will check after 60 seconds.
        """
        # query for backups whose scheduled date is before current date minus
        # than max starvation time


        where = ("(Math.min(%s, (this.plan.schedule.frequencyInSeconds / 2) * 1000) + "
                    "this.createdDate.getTime()) < new Date().getTime()" %
                 (MAX_BACKUP_WAIT_TIME * 1000))
        one_off_starve_date = date_minus_seconds(date_now(),
                                                 ONE_OFF_BACKUP_MAX_WAIT_TIME)
        q = {
            "state": STATE_SCHEDULED,
            "$or":[
                # backups with plans starving query
                {
                    "$and":[
                        {"plan": {"$exists": True}},
                        {"$where": where}
                    ]
                },
                # One off backups (no plan) starving query
                {
                    "$and":[
                            {"plan": {"$exists": False}},
                            {"createdDate": {"$lt": one_off_starve_date}}
                    ]
                 }
            ]
        }

        starving_backups = get_mbs().backup_collection.find(q)

        if starving_backups:
            msg = ("You have %s scheduled backups that has past the maximum "
                   "waiting time (%s seconds)." %
                   (len(starving_backups), MAX_BACKUP_WAIT_TIME))
            self.info(msg)


            self.info("Sending a notification...")
            sbj = "Past due scheduled backups"
            get_mbs().send_notification(sbj, msg)

    ###########################################################################
    def _resolve_task_tags(self, task, tags):
        if tags:
            tag_vals = {}
            for name,value in tags.items():
                if isinstance(value, DynamicTag):
                    tag_vals[name] = value.generate_tag_value(task)
                else:
                    tag_vals[name] = value

            return tag_vals

    ###########################################################################
    def _notify_on_late_in_progress_backups(self):
        """
            Send notifications for jobs that have been in progress for a period
            longer than a MAX_BACKUP_WAIT_TIME threshold
        """

        min_start_date = date_minus_seconds(date_now(), MAX_BACKUP_WAIT_TIME)
        q = {
            "state": STATE_IN_PROGRESS,
            "startDate": {
                "$lt": min_start_date
            }
        }

        late_backups = get_mbs().backup_collection.find(q)

        if late_backups:
            msg = ("You have %s in-progress backups that has been running for"
                   " more than the maximum waiting time (%s seconds)." %
                   (len(late_backups), MAX_BACKUP_WAIT_TIME))
            self.info(msg)


            self.info("Sending a notification...")
            sbj = "Late in-progress backups"
            get_mbs().send_notification(sbj, msg)

    ###########################################################################
    def _notify_error(self, exception):
        subject = "BackupSystem Error"
        message = ("BackupSystem Error!.\n\nStack Trace:\n%s" %
                   traceback.format_exc())

        get_mbs().send_error_notification(subject, message, exception)

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
        return resolve_path(os.path.join(mbs_config.MBS_CONF_DIR,
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
        url = "http://0.0.0.0:%s/status" % self._api_port
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
            raise BackupSystemError("Backup system did not stop in 60 seconds")

    ###########################################################################
    def _do_get_status(self):
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
            "status": status
        }

    ###########################################################################
    # api server
    ###########################################################################

    def _start_api_server(self):
        self.info("Starting api server at port %s" % self._api_port)

        self.api_server.start()
        self.info("api server started successfully!")

    ###########################################################################
    # Backup sweeper
    ###########################################################################

    def _start_backup_sweeper(self):
        #self.backup_sweeper.start()
        pass
    def _stop_backup_sweeper(self):
        #self.backup_sweeper.stop()
        pass
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
