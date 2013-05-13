__author__ = 'abdul'

import time
import mbs_logging
import traceback
import urllib
import json
import os

from threading import Thread

from flask import Flask
from flask.globals import request
from utils import document_pretty_string, resolve_path

import mbs_config

from date_utils import date_now, date_minus_seconds, time_str_to_datetime_today
from errors import *
from auditors import GlobalAuditor
from backup import (Backup, STATE_SCHEDULED, STATE_IN_PROGRESS, STATE_FAILED,
                    STATE_CANCELED, EVENT_STATE_CHANGE)

from persistence import update_backup, expire_backup

from mongo_utils import objectiditify

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
                       command_port=9003):

        Thread.__init__(self)
        self._plan_collection = None
        self._backup_collection = None
        self._sleep_time = sleep_time

        self._plan_generators = []
        self._tick_count = 0
        self._notification_handler = None
        self._stopped = False
        self._command_port = command_port
        self._command_server = BackupSystemCommandServer(self)

        # auditing stuff
        self._audit_collection = None

        # init global editor
        self._audit_notification_handler = None
        self._auditors = None
        self._global_auditor = None
        self._audit_schedule = None
        self._audit_next_occurrence = None

    ###########################################################################
    # Properties
    ###########################################################################
    @property
    def plan_collection(self):
        return self._plan_collection

    @plan_collection.setter
    def plan_collection(self, pc):
        self._plan_collection = pc

    ###########################################################################
    @property
    def backup_collection(self):
        return self._backup_collection

    @backup_collection.setter
    def backup_collection(self, bc):
        self._backup_collection = bc

    ###########################################################################
    @property
    def plan_generators(self):
        return self._plan_generators

    @plan_generators.setter
    def plan_generators(self, value):
        self._plan_generators = value

    ###########################################################################
    @property
    def notification_handler(self):
        return self._notification_handler

    @notification_handler.setter
    def notification_handler(self, handler):
        self._notification_handler = handler

    ###########################################################################
    @property
    def audit_collection(self):
        return self._audit_collection

    @audit_collection.setter
    def audit_collection(self, ac):
        self._audit_collection = ac

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
            ac = self.audit_collection
            nh = self.audit_notification_handler
            self._global_auditor = GlobalAuditor(audit_collection=ac,
                                                 notification_handler=nh)
            # register auditors with global auditor
            if self.auditors:
                for auditor in self.auditors:
                    self._global_auditor.register_auditor(auditor)

        return self._global_auditor

    ###########################################################################
    # Behaviors
    ###########################################################################
    def run(self):
        self.info("Starting up... ")
        self.info("PID is %s" % os.getpid())
        self._update_pid_file()

        # Start the command server
        self._start_command_server()

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

            self._set_plan_next_occurrence(plan)

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

            self.schedule_new_backup(plan)


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


        return self._plan_collection.find(q)

    ###########################################################################
    def _plan_has_backup_in_progress(self, plan):
        q = {
            "plan.$id": plan._id,
            "state": STATE_IN_PROGRESS
        }
        return self._backup_collection.find_one(q) is not None

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

        for backup in self._backup_collection.find(q):
            self.info("Cancelling backup %s" % backup._id)
            backup.state = STATE_CANCELED
            update_backup(backup, properties="state",
                          event_name=EVENT_STATE_CHANGE,
                          message="Backup is past due. Canceling...")

    ###########################################################################
    def _reschedule_in_cycle_failed_backups(self):
        """
        Reschedule failed reschedulable backups that failed at least
        RESCHEDULE_PERIOD seconds ago
        """
        now = date_now()

        # select backups whose last log date is at least RESCHEDULE_PERIOD ago

        where = ("(this.logs[this.logs.length-1].date.getTime() + %s) < "
                 "new Date().getTime()" % RESCHEDULE_PERIOD_MILLS)
        q = {
            "state": STATE_FAILED,
            "reschedulable": True,
            "$where": where
        }

        for backup in self._backup_collection.find(q):
            self.reschedule_backup(backup)

    ###########################################################################
    def reschedule_all_failed_backups(self, from_scratch=False):
        self.info("Rescheduling all failed backups")

        q = {
            "state": STATE_FAILED
        }

        for backup in self._backup_collection.find(q):
            self.reschedule_backup(backup, from_scratch=from_scratch)

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
            backup.tags = backup.plan.generate_tags()

        # if from_scratch is set then clear backup log
        if from_scratch:
            backup.logs = []
            backup.try_count = 0
            update_backup(backup, properties=["logs", "tryCount"])

        update_backup(backup, properties=["state", "tags"],
                      event_name=EVENT_STATE_CHANGE,
                      message="Rescheduling")

    ###########################################################################
    def schedule_new_backup(self, plan, one_time=False):
        self.info("Scheduling plan '%s'" % plan._id)

        backup = Backup()
        backup.created_date = date_now()
        backup.strategy = plan.strategy
        backup.source = plan.source
        backup.target = plan.target
        backup.tags = plan.generate_tags()
        backup.priority = plan.priority
        backup.change_state(STATE_SCHEDULED)
        if not one_time:
            backup.plan_occurrence = plan.next_occurrence
            self._set_plan_next_occurrence(plan)
            backup.plan = plan
        backup_doc = backup.to_document()
        self._backup_collection.save_document(backup_doc)
        # set the backup id from the saved doc
        backup.id = backup_doc["_id"]

        self.info("Scheduled backup \n%s" % backup)
        return backup

    ###########################################################################
    def _set_plan_next_occurrence(self, plan):
        plan.next_occurrence = plan.next_natural_occurrence()
        q = {"_id": plan.id}
        u = {
            "$set": {
                "nextOccurrence": plan.next_occurrence
            }
        }
        self._plan_collection.update(spec=q, document=u)

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

            self._plan_collection.save_document(plan.to_document())

            self.info("Plan saved successfully")
        except Exception, e:
            raise BackupSystemError("Error while saving plan %s. %s" %
                                       (plan, e))

    ###########################################################################
    def remove_plan(self, plan):
        logger.info("Removing plan '%s' " % plan.id)
        self._plan_collection.remove_by_id(plan.id)

    ###########################################################################
    def delete_backup(self, backup_id):
        """
            Deletes the specified backup. Deleting here means expiring
        """
        backup_id = objectiditify(backup_id)

        backup = self.backup_collection.find_one(backup_id)
        if (backup.target_reference and
            not backup.target_reference.expired_date):
            expire_backup(backup, date_now())
            return True

        return False

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
                    "this.logs[0].date.getTime()) < new Date().getTime()" %
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
                            {"logs.0.date": {"$lt": one_off_starve_date}}
                    ]
                 }
            ]
        }

        starving_backups = self._backup_collection.find(q)

        if starving_backups:
            msg = ("You have %s scheduled backups that has past the maximum "
                   "waiting time (%s seconds)." %
                   (len(starving_backups), MAX_BACKUP_WAIT_TIME))
            self.info(msg)

            if self._notification_handler:
                self.info("Sending a notification...")
                sbj = "Past due scheduled backups"
                self._notification_handler.send_notification(sbj, msg)

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

        late_backups = self._backup_collection.find(q)

        if late_backups:
            msg = ("You have %s in-progress backups that has been running for"
                   " more than the maximum waiting time (%s seconds)." %
                   (len(late_backups), MAX_BACKUP_WAIT_TIME))
            self.info(msg)

            if self._notification_handler:
                self.info("Sending a notification...")
                sbj = "Late in-progress backups"
                self._notification_handler.send_notification(sbj, msg)

    ###########################################################################
    def _notify_error(self, exception):
        if self._notification_handler:
            subject = "BackupSystem Error"
            message = ("BackupSystem Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())

            nh = self._notification_handler
            nh.send_error_notification(subject, message, exception)

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
    def stop(self, force=False):
        """
            Sends a stop request to the backup system using the command port
            This should be used by other processes (copy of the backup system
            instance) but not the actual running backup system process
        """

        if force:
            self._kill_backup_system_process()
            return

        url = "http://0.0.0.0:%s/stop" % self._command_port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                print response.read().strip()
            else:
                msg =  ("Error while trying to stop backup systemURL %s "
                        "(Response"" code %)" %
                        ( url, response.getcode()))
                raise BackupSystemError(msg)
        except IOError, e:
            logger.error("BackupSystem is not running")

    ###########################################################################
    def get_status(self):
        """
            Sends a status request to the backup system using the command port
            This should be used by other processes (copy of the backup system
            instance) but not the actual running backup system process
        """
        url = "http://0.0.0.0:%s/status" % self._command_port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                return json.loads(response.read().strip())
            else:
                msg =  ("Error while trying to get status backup system URL"
                        " %s (Response code %)" % (url, response.getcode()))
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
        self._stopped = True

    ###########################################################################
    def _do_get_status(self):
        """
            Gets the status of the backup system
        """
        if self._stopped:
            status = BACKUP_SYSTEM_STATUS_STOPPING
        else:
            status = BACKUP_SYSTEM_STATUS_RUNNING

        return {
            "status": status
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
# BackupSystemCommandServer
###############################################################################
class BackupSystemCommandServer(Thread):

    ###########################################################################
    def __init__(self, backup_system):
        Thread.__init__(self)
        self._backup_system = backup_system
        self._flask_server = self._build_flask_server()

    ###########################################################################
    def _build_flask_server(self):
        flask_server = Flask(__name__)
        backup_system = self._backup_system

        ########## build stop method
        @flask_server.route('/stop', methods=['GET'])
        def stop_backup_system():
            logger.info("Command Server: Received a stop command")
            try:
                backup_system._do_stop()
                return "BackupSystem stopped successfully"
            except Exception, e:
                return "Error while trying to stop backup system: %s" % e

        ########## build status method
        @flask_server.route('/status', methods=['GET'])
        def status():
            logger.info("Command Server: Received a status command")
            try:
                return document_pretty_string(backup_system._do_get_status())
            except Exception, e:
                return "Error while trying to get backup system status: %s" % e

        ########## build delete backup method
        @flask_server.route('/delete-backup/<backup_id>', methods=['GET'])
        def delete_backup(backup_id):
            logger.info("Command Server: Received a delete-backup command")
            try:
                return str(backup_system.delete_backup(backup_id))
            except Exception, e:
                return "Error while trying to get backup system status: %s" % e

        ########## build stop-command-server method
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
                return "Error while trying to get backup system status: %s" % e

        return flask_server

    ###########################################################################
    def run(self):
        logger.info("BackupSystemCommandServer: Running flask server ")
        self._flask_server.run(host="0.0.0.0",
                               port=self._backup_system._command_port,
                               threaded=True)

    ###########################################################################
    def stop(self):

        logger.info("BackupSystemCommandServer: Stopping flask server ")
        port = self._backup_system._command_port
        url = "http://0.0.0.0:%s/stop-command-server" % port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                logger.info("BackupSystemCommandServer: Flask server stopped "
                            "successfully")
                return response.read().strip()
            else:
                msg =  ("Error while trying to send command of URL %s "
                        "(Response code %)" % (url, response.getcode()))
                raise BackupSystemError(msg)

        except Exception, e:
            raise BackupSystemError("Error while stopping flask server:"
                                        " %s" %e)
