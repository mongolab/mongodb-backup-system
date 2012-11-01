__author__ = 'abdul'

import time
import mbs_logging
import traceback
from threading import Thread

from flask import Flask
from flask.globals import request

from utils import date_now
from errors import MBSException

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
# PlanManager
###############################################################################
class PlanManager(Thread):
    ###########################################################################
    def __init__(self, plan_collection, backup_collection,
                       sleep_time=10, notification_handler=None,
                       command_port=9999):

        Thread.__init__(self)
        self._plan_collection = plan_collection
        self._backup_collection = backup_collection
        self._sleep_time = sleep_time

        self._registered_plan_generators = []
        self._tick_ring = 0
        self._notification_handler = notification_handler
        self._stopped = False
        self._command_port = command_port
        self._command_server = ManagerCommandServer(self)

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

    ###########################################################################
    def _tick(self):

        # increase _generators_tick_counter
        self._tick_ring = (self._tick_ring + 1) % 10

        self._process_failed_backups()
        self._process_plans_considered_now()

        if self._tick_ring == 0:
            self._run_plan_generators()

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
        Cancels backups that failed and whose plan's next occurrence
         in in the past
        """
        now = date_now()

        #self.info("Cancelling failed backups whose"
        #                " next occurrence is very soon or in the past")

        q = {
            "state": STATE_FAILED,
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
    # plan generators methods a
    ###########################################################################
    def register_plan_generator(self, generator):
        self._registered_plan_generators.append(generator)

    ###########################################################################
    def _run_plan_generators(self):
        for generator in self._registered_plan_generators:
                self._run_generator(generator)

    ###########################################################################
    def _run_generator(self, generator):

        # remove expired plans
        for plan in generator.get_plans_to_remove():
            self.remove_plan(plan)

        # save new plans
        for plan in generator.get_plans_to_save():
            self.save_plan(plan)

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
            Stops the manager gracefully by waiting for the current tick to
             finish
        """
        self.info("Stopping manager gracefully. Waiting current tick"
                  " to finish")

        self._stopped = True
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
        @flask_server.route('/stop', methods=['GET'])
        def stop_engine():
            logger.info("Command Server: Received a stop command")
            try:
                manager.stop()
                return "Manager stopped successfully"
            except Exception, e:
                return "Error while trying to stop manager: %s" % e

        return flask_server

    ###########################################################################
    def run(self):
        logger.info("ManagerCommandServer: Running flask server ")
        self._flask_server.run(host="0.0.0.0",
                               port=self._manager._command_port,
                               threaded=True)
        ###########################################################################
    def stop(self):
        """
            Stops the flask server
            http://flask.pocoo.org/snippets/67/
        """
        logger.info("EngineCommandServer: Stopping flask server ")
        shutdown = request.environ.get('werkzeug.server.shutdown')
        if shutdown is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        shutdown()
        logger.info("EngineCommandServer: Flask server stopped successfully")

###############################################################################
# PlanManagerException
###############################################################################
class PlanManagerException(MBSException):

    ###########################################################################
    def __init__(self, message, cause=None):
        MBSException.__init__(self, message, cause=cause)