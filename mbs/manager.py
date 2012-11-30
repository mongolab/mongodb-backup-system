__author__ = 'abdul'

import time
import mbs_logging
import traceback
import urllib
import json

from threading import Thread

from flask import Flask
from flask.globals import request
from utils import document_pretty_string

from date_utils import date_now, date_minus_seconds, time_str_to_datetime_today
from errors import MBSException
from audit import GlobalAuditor, PlanAuditor
from backup import (Backup, STATE_SCHEDULED, STATE_IN_PROGRESS, STATE_FAILED,
                    STATE_CANCELED)
###############################################################################
########################                                #######################
######################## Plan Management and Scheduling #######################
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

MANAGER_STATUS_RUNNING = "running"
MANAGER_STATUS_STOPPING = "stopping"
MANAGER_STATUS_STOPPED = "stopped"

###############################################################################
# PlanManager
###############################################################################
class PlanManager(Thread):
    ###########################################################################
    def __init__(self, sleep_time=10,
                       command_port=9003):

        Thread.__init__(self)
        self._plan_collection = None
        self._backup_collection = None
        self._sleep_time = sleep_time

        self._registered_plan_generators = []
        self._tick_ring = 0
        self._notification_handler = None
        self._stopped = False
        self._command_port = command_port
        self._command_server = ManagerCommandServer(self)

        # auditing stuff
        self._audit_collection = None

        # init global editor
        self._audit_notification_handler = None
        self._global_auditor = None
        self._plan_auditor = None
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
            # create / register plan auditor
            plan_auditor = PlanAuditor(self.plan_collection,
                                       self.backup_collection)
            self._global_auditor.register_auditor(plan_auditor)

        return self._global_auditor

    ###########################################################################
    # Behaviors
    ###########################################################################
    def run(self):
        self.info("Starting up... ")
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
        self._tick_ring = (self._tick_ring + 1) % 10

        self._process_failed_backups()
        self._process_plans_considered_now()

        if self._tick_ring == 0:
            self._run_plan_generators()
            self._check_starving_scheduled_backups()

        # run auditor if its time
        #self._check_audit()

    ###########################################################################
    def _process_plans_considered_now(self):
        for plan in self._get_plans_to_consider_now():
            self._process_plan(plan)

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
            self.error(err_msg)
            return
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

            self._schedule_new_backup(plan)


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
    def _process_failed_backups(self):
        self._cancel_failed_backups_not_within_current_cycle()
        # temporarily turn this off until we introduce retryTimeout/maxNoRetries
        #self._reschedule_failed_backups_within_current_cycle()

    ###########################################################################
    def _cancel_failed_backups_not_within_current_cycle(self):
        """
        Cancels backups that failed or scheduled
        and whose plan's next occurrence in in the past
        """
        now = date_now()

        #self.info("Cancelling failed backups whose"
        #                " next occurrence is very soon or in the past")

        q = {
            "state": {"$in": [STATE_FAILED, STATE_SCHEDULED]},
            "plan.nextOccurrence": {"$lte": now}
        }

        for backup in self._backup_collection.find(q):
            self.info("Cancelling backup %s" % backup._id)
            backup.change_state(STATE_CANCELED)
            self._backup_collection.save_document(backup.to_document())

    ###########################################################################
    def _reschedule_failed_backups_within_current_cycle(self):
        """
        Reschedule backups that failed and whose plan's next occurrence
         is in the future
        """
        now = date_now()

        #self.info("Rescheduling failed backups whose next occurrence is"
        #                " in the future")

        q = {
            "state": STATE_FAILED,
            "plan.nextOccurrence": {"$gt": now}
        }

        for backup in self._backup_collection.find(q):

            self.info("Rescheduling backup %s" % backup._id)
            backup.change_state(STATE_SCHEDULED)
            self._backup_collection.save_document(backup.to_document())

    ###########################################################################
    def _schedule_new_backup(self, plan):
        self.info("Scheduling plan '%s'" % plan._id)

        backup = Backup()
        backup.strategy = plan.strategy
        backup.source = plan.source
        backup.target = plan.target
        backup.tags = plan.tags
        backup.plan_occurrence = plan.next_occurrence
        backup.change_state(STATE_SCHEDULED)
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
        self._plan_collection.save_document(plan.to_document())

    ###########################################################################
    def save_plan(self, plan):
        try:

            self.debug("Validating plan %s" % plan)
            errors = plan.validate()
            if errors:
                err_msg = ("Plan %s is invalid."
                           "Please correct the following errors and then try"
                           " saving again.\n%s" % (plan, errors))

                raise PlanManagerException(err_msg)

            if plan.id:
                self.info("Updating plan: \n%s" % plan)
            else:
                self.info("Saving new plan: \n%s" % plan)

            self._plan_collection.save_document(plan.to_document())

            self.info("Plan saved successfully")
        except Exception, e:
            raise PlanManagerException("Error while saving plan %s. %s" %
                                       (plan, e))

    ###########################################################################
    def remove_plan(self, plan):
        logger.info("Removing plan '%s' " % plan.id)
        self._plan_collection.remove_by_id(plan.id)

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
    # plan generators methods a
    ###########################################################################
    def register_plan_generator(self, generator):
        self._registered_plan_generators.append(generator)

    ###########################################################################
    def _run_plan_generators(self):
        self.info("Running ALL plan generators")
        for generator in self._registered_plan_generators:
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
    def _check_starving_scheduled_backups(self):
        """
            Send notifications for jobs that has been scheduled for a long
            time and have not been picked up by an engine yet
        """
        # query for backups whose scheduled date is before current date minus
        # than max starvation time
        starve_date = date_minus_seconds(date_now(), MAX_BACKUP_WAIT_TIME)
        q = {
            "state": STATE_SCHEDULED,
            "logs.0.date": {"$lt": starve_date}
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
    def _notify_error(self, exception):
        if self._notification_handler:
            subject = "PlanManager Error"
            message = ("PlanManager Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())

            self._notification_handler.send_notification(subject, message)

    ###########################################################################
    # Manager stopping
    ###########################################################################
    def stop(self):
        """
            Sends a stop request to the manager using the command port
            This should be used by other processes (copy of the manager
            instance) but not the actual running manager process
        """

        url = "http://0.0.0.0:%s/stop" % self._command_port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                print response.read().strip()
            else:
                msg =  ("Error while trying to stop manager URL %s "
                        "(Response"" code %)" %
                        ( url, response.getcode()))
                raise PlanManagerException(msg)
        except IOError, e:
            logger.error("Manager is not running")

    ###########################################################################
    def get_status(self):
        """
            Sends a status request to the manager using the command port
            This should be used by other processes (copy of the manager
            instance) but not the actual running manager process
        """
        url = "http://0.0.0.0:%s/status" % self._command_port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                return json.loads(response.read().strip())
            else:
                msg =  ("Error while trying to get status manager URL %s "
                        "(Response code %)" % (url, response.getcode()))
                raise PlanManagerException(msg)

        except IOError, ioe:
            return {
                "status": MANAGER_STATUS_STOPPED
            }

    ###########################################################################
    def _do_stop(self):
        """
            Triggers the manager to gracefully stop
        """
        self.info("Stopping manager gracefully")
        self._stopped = True

    ###########################################################################
    def _do_get_status(self):
        """
            Gets the status of the manager
        """
        if self._stopped:
            status = MANAGER_STATUS_STOPPING
        else:
            status = MANAGER_STATUS_RUNNING

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
        logger.info("PlanManager: %s" % msg)

    ###########################################################################
    def error(self, msg):
        logger.error("PlanManager: %s" % msg)

    ###########################################################################
    def debug(self, msg):
        logger.debug("PlanManager: %s" % msg)


###############################################################################
# ManagerCommandServer
###############################################################################
class ManagerCommandServer(Thread):

    ###########################################################################
    def __init__(self, manager):
        Thread.__init__(self)
        self._manager = manager
        self._flask_server = self._build_flask_server()

    ###########################################################################
    def _build_flask_server(self):
        flask_server = Flask(__name__)
        manager = self._manager

        ## build stop method
        @flask_server.route('/stop', methods=['GET'])
        def stop_manager():
            logger.info("Command Server: Received a stop command")
            try:
                manager._do_stop()
                return "Manager stopped successfully"
            except Exception, e:
                return "Error while trying to stop manager: %s" % e

        ## build status method
        @flask_server.route('/status', methods=['GET'])
        def status():
            logger.info("Command Server: Received a status command")
            try:
                return document_pretty_string(manager._do_get_status())
            except Exception, e:
                return "Error while trying to get manager status: %s" % e

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
                return "Error while trying to get manager status: %s" % e

        return flask_server

    ###########################################################################
    def run(self):
        logger.info("ManagerCommandServer: Running flask server ")
        self._flask_server.run(host="0.0.0.0",
                               port=self._manager._command_port,
                               threaded=True)

    ###########################################################################
    def stop(self):

        logger.info("ManagerCommandServer: Stopping flask server ")
        port = self._manager._command_port
        url = "http://0.0.0.0:%s/stop-command-server" % port
        try:
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                logger.info("ManagerCommandServer: Flask server stopped "
                            "successfully")
                return response.read().strip()
            else:
                msg =  ("Error while trying to send command of URL %s "
                        "(Response code %)" % (url, response.getcode()))
                raise PlanManagerException(msg)

        except Exception, e:
            raise PlanManagerException("Error while stopping flask server:"
                                        " %s" %e)

###############################################################################
# PlanManagerException
###############################################################################
class PlanManagerException(MBSException):

    ###########################################################################
    def __init__(self, message, cause=None):
        MBSException.__init__(self, message, cause=cause)