__author__ = 'abdul'

import sys
import os
import logging
import time
import mbs_logging

from threading import Thread

from utils import date_now
from backup import (Backup, STATE_SCHEDULED, STATE_IN_PROGRESS, STATE_FAILED,
                    STATE_SUCCEEDED, STATE_CANCELED)
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
    def __init__(self, plan_collection, backup_collection, sleep_time=10):

        Thread.__init__(self)
        self._plan_collection = plan_collection
        self._backup_collection = backup_collection
        self._sleep_time = sleep_time

        self._registered_plan_generators = []
        self._tick_ring = 0

    ###########################################################################
    def run(self):
        self.info("Starting up... ")
        while True:
            self._tick()
            time.sleep(self._sleep_time)

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
            errors_str = "\n".join(errors)
            err_msg = ("Plan '%s' is invalid and will be Deactivated."
                   "Please correct the following errors and then set active"
                   " to true.\n%s" % (plan.id, errors_str))
            self.error(err_msg)
            plan.errors = errors
            self._deactivate_plan(plan)
            return

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
        q = {"$and": [
                {"$or": [
                    {"active": {"$exists": False}},
                    {"active": True}
                ]},
                {"$or": [
                    {"nextOccurrence": {"$exists": False}},
                    {"nextOccurrence": None},
                    {"nextOccurrence": {"$lte": now}}
                ]}

        ]}


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
        self._reschedule_failed_backups_within_current_cycle()

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
    def _last_backup(self, plan):
        backup_coll = self._backup_collection
        results = (backup_coll.find(query={"plan.$id": plan._id},
                                    sort=("timestamp", -1),
                                    limit=1))

        if results is not None and results.count(True) > 0:
            return results[0]

    ###########################################################################
    def _schedule_new_backup(self, plan):
        self.info("Scheduling plan '%s'" % plan._id)

        backup = Backup()
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
    def _deactivate_plan(self, plan):
        try:

            self.info("Deactivating plan '%s' Setting plan.active to false" %
                      plan.id)
            plan.active = False


            self._plan_collection.save_document(plan.to_document())
            self.info("Plan deactivated successfully")
        except Exception, e:
            self.error("Error while deactivating plan '%s'. %s" % (plan.id, e))


    ###########################################################################
    def save_plan(self, plan):
        try:

            self.info("Validating plan....")
            plan.validate()
            self.info("Saving plan: \n%s" % plan)

            self._plan_collection.save_document(plan.to_document())

            self.info("Plan saved successfully")
        except Exception, e:
            self.error("Error while saving plan '%s'. %s" % (plan.id, e))

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

        # save new plans
        for plan in generator.get_new_plans():
            self.info("Getting newly generated plan:\n%s " % plan)
            self.save_plan(plan)

        # remove expired plans
        for plan in generator.get_expired_plans():
            self.info("Removing expired plan '%s' " % plan.id)
            self.remove_plan(plan)

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