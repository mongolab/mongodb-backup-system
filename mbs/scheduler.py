__author__ = 'abdul'

from schedule_runner import ScheduleRunner
from schedule import Schedule
from globals import State
from mbs import get_mbs
from date_utils import date_now, timedelta_total_seconds
from task import EVENT_STATE_CHANGE
import traceback
import logging
from errors import InvalidPlanError

import Queue
########################################################################################################################
# LOGGER
########################################################################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

PLAN_WORKER_COUNT = 5
########################################################################################################################
class BackupScheduler(ScheduleRunner):
    """
        Backup monitoring thread
    """
    ####################################################################################################################
    def __init__(self, backup_system):
        self._backup_system = backup_system
        ScheduleRunner.__init__(self, schedule=Schedule(frequency_in_seconds=10))
        self._plans_queue = Queue.Queue()
        self._plan_workers = None

    ####################################################################################################################
    def run(self):
        self._init_workers()
        super(BackupScheduler,self).run()

    ####################################################################################################################
    def _init_workers(self):
        self._plan_workers = []
        for i in range(0, PLAN_WORKER_COUNT):
            worker = PlanWorker(self, self._plans_queue)
            self._plan_workers.append(worker)
            worker.start()

    ####################################################################################################################
    def tick(self):

        try:
            self._process_plans_considered_now(process_max_count=100)
            self._cancel_past_cycle_backups()
            self._process_failed_backups()
        except Exception, e:
            logger.error("Caught an error: '%s'.\nStack Trace:\n%s" %
                       (e, traceback.format_exc()))
            self._backup_system._notify_error(e)

    ####################################################################################################################
    def _process_plans_considered_now(self, process_max_count=None):
        count = 0
        start_date = date_now()
        for plan in self._get_plans_to_consider_now(limit=process_max_count):
            self._plans_queue.put(plan)
            if process_max_count:
                count += 1
                if count >= process_max_count:
                    break
        # wait for workers to finish
        self._plans_queue.join()

        if count:
            time_elapsed = timedelta_total_seconds(date_now() - start_date)
            logger.info("Finished processing %s plans in %s seconds" % ((count or "all"), time_elapsed))

    ####################################################################################################################
    def _process_plan(self, plan):
        """
        Schedule the plan if the following conditions apply
        """
        logger.info("Processing plan '%s'" % plan.id)
        # validate plan first
        logger.debug("Validating plan '%s'" % plan.id)

        errors = plan.validate()
        if errors:
            err_msg = ("Plan '%s' is invalid. Deleting...."
                       " errors.\n%s" % (plan.id, errors))
            logger.error(err_msg)
            self._backup_system.remove_plan(plan.id)

            return


        now = date_now()
        next_natural_occurrence = plan.schedule.next_natural_occurrence()

        # CASE I: First time <==> No previous backups
        # Only set the next occurrence here
        if not plan.next_occurrence:
            logger.info("Plan '%s' has no previous backup. Setting next occurrence to '%s'" %
                        (plan.id, next_natural_occurrence))

            self._set_update_plan_next_occurrence(plan)

            # CASE II: If there is a backup running (IN PROGRESS)
            # ===> no op
            ### TODO XXX : we don't this case any more

            #elif self._plan_has_backup_in_progress(plan):
            #self.info("Plan '%s' has a backup that is currently in"
            #" progress. Nothing to do now." % plan._id)
        # CASE III: if time now is past the next occurrence
        elif plan.next_occurrence <= now:
            logger.info("Plan '%s' next occurrence '%s' is greater than"
                      " now. Scheduling a backup!!!" %
                      (plan.id, plan.next_occurrence))

            self._backup_system.schedule_plan_backup(plan)
        else:
            logger.info("Wooow. How did you get here!!!! Plan '%s' does not to be scheduled yet. next natural "
                        "occurrence %s " % (plan.id, next_natural_occurrence))

    ####################################################################################################################
    def _get_plans_to_consider_now(self, limit=None):
        """
        Returns list of plans that the scheduler should process at this time.
        Those are:
            1- Plans with no backups scheduled yet (next occurrence has not
            been calculated yet)

            2- Plans whose next occurrence is now or in the past

        """
        now = date_now()
        q = {"$or": [
            {"nextOccurrence": None},
            {"nextOccurrence": {"$lte": now}}
        ]
        }

        # sort by priority
        s = [("priority", 1)]

        return get_mbs().plan_collection.find_iter(q, sort=s, limit=limit)

    ####################################################################################################################
    def _set_update_plan_next_occurrence(self, plan):
        plan.next_occurrence = plan.schedule.next_natural_occurrence()
        self._save_plan_next_occurrence(plan)

    ####################################################################################################################
    def _save_plan_next_occurrence(self, plan):
        q = {"_id": plan.id}
        u = {
            "$set": {
                "nextOccurrence": plan.next_occurrence
            }
        }
        get_mbs().plan_collection.update(spec=q, document=u)

    ####################################################################################################################
    def _cancel_past_cycle_backups(self):
        """
        Cancels scheduled backups (or backups failed to be scheduled,
         i.e. engine guid is none) whose plan's next occurrence in in the past
        """
        now = date_now()

        q = {
            "state": {"$in": [State.SCHEDULED, State.FAILED]},
            "plan.nextOccurrence": {"$lte": now},
            "engineGuid": None
        }

        bc = get_mbs().backup_collection
        for backup in bc.find(q):
            logger.info("Cancelling backup %s" % backup.id)
            backup.state = State.CANCELED
            bc.update_task(backup, properties="state",
                           event_name=EVENT_STATE_CHANGE,
                           message="Backup is past due. Canceling...")

    ####################################################################################################################
    def _process_failed_backups(self):
        """
        Reschedule failed backups that failed and are retriable
        """

        q = {
            "state": State.FAILED,
            "nextRetryDate": {
                "$lt": date_now()
            }
        }

        for backup in get_mbs().backup_collection.find(q):
            self._process_failed_backup(backup)


    ####################################################################################################################
    def _process_failed_backup(self, backup):
        """

        :param backup:
        :return:
        """
        if backup.next_retry_date and backup.next_retry_date < date_now() :
            # RESCHEDULE !!!
            self._backup_system.reschedule_backup(backup)

    ####################################################################################################################
    def _plan_has_backup_in_progress(self, plan):
        q = {
            "plan._id": plan.id,
            "state": State.IN_PROGRESS
        }
        return get_mbs().backup_collection.find_one(q) is not None

    ####################################################################################################################


#########################################################################################################################

PLAN_WORKER_SCHEDULE = Schedule(frequency_in_seconds=1)

class PlanWorker(ScheduleRunner):
    """
        A Thread that periodically expire backups that are due for expiration
    """
    ####################################################################################################################
    def __init__(self, scheduler, plan_queue):
        ScheduleRunner.__init__(self, schedule=PLAN_WORKER_SCHEDULE)
        self._scheduler = scheduler
        self._plan_queue = plan_queue

    ####################################################################################################################
    def tick(self):
        while True:

            try:
                plan = self._plan_queue.get_nowait()
            except Queue.Empty:
                # breaking
                break
            try:
                self._scheduler._process_plan(plan)
            except Exception, e:
                logger.exception("Error while processing plan '%s'. "
                                 "Cause: %s" % (plan.id, e))

                subject = "Plan Scheduler Error"
                message = ("Error while processing plan '%s'. Cause: %s.\n\nStack Trace:\n%s" %
                           (plan.id, e, traceback.format_exc()))
                get_mbs().notifications.send_error_notification(subject, message)
            finally:
                self._plan_queue.task_done()
