__author__ = 'abdul'

from schedule_runner import ScheduleRunner
from schedule import Schedule
from globals import State
from mbs import get_mbs
from date_utils import date_now, date_plus_seconds, mid_date_between
from task import EVENT_STATE_CHANGE
import traceback
import logging
from errors import InvalidPlanError, is_exception_retriable
from persistence import update_backup
########################################################################################################################
# LOGGER
########################################################################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

########################################################################################################################
class BackupScheduler(ScheduleRunner):
    """
        Backup monitoring thread
    """
    ####################################################################################################################
    def __init__(self, backup_system):
        self._backup_system = backup_system
        ScheduleRunner.__init__(self, schedule=Schedule(frequency_in_seconds=10))

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
        for plan in self._get_plans_to_consider_now(limit=process_max_count):
            try:
                self._process_plan(plan)
            except Exception, e:
                logger.exception("Error while processing plan '%s'. "
                                 "Cause: %s" % (plan.id, e))

                self._backup_system._notify_error(e)
            if process_max_count:
                count += 1
                if count >= process_max_count:
                    break

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
            err_msg = ("Plan '%s' is invalid.Please correct the following"
                       " errors.\n%s" % (plan.id, errors))
            raise InvalidPlanError(err_msg)
            # TODO disable plan ???

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
            "state": State.FAILED
        }

        for backup in get_mbs().backup_collection.find(q):
            self._process_failed_backup(backup)


    ####################################################################################################################
    def _process_failed_backup(self, backup):
        """
        Handles failed backups
        1- Updates backup next retry and final retry
        2- reschedules backups whose next retry is less than now
        2- NOOP on failed backups whose final retry is less than now
        :param backup:
        :return:
        """
        if backup.final_retry_date and date_now() > backup.final_retry_date:
            # NOOP
            pass
        elif (not backup.final_retry_date or
            (backup.next_retry_date is None and backup.final_retry_date > date_now())):
            self._update_failed_backup_retry_info(backup)

        elif backup.next_retry_date and backup.next_retry_date < date_now():
            # RESCHEDULE !!!
            self._backup_system.reschedule_backup(backup)

    ####################################################################################################################
    def _update_failed_backup_retry_info(self, backup):
        """
        Backup retry logic

        :param backup:
        :return:
        """

        last_error_code = backup.get_last_error_code()

        # if exception is not retriable then mark backup is not retriable by setting final retry to now
        if not is_exception_retriable(last_error_code):
            logger.info("Last error for backup %s is not retriable. Marking backup is not retriable."
                        " Setting finalRetryDate to now...")
            backup.final_retry_date = date_now()
            backup.next_retry_date = None
        else:
            # compute final retry date
            if not backup.final_retry_date:
                backup.final_retry_date = self._compute_final_retry_date(backup)

            next_retry_date = self._compute_next_retry_date(backup)
            if next_retry_date <= backup.final_retry_date:
                backup.next_retry_date = next_retry_date
            else:
                backup.next_retry_date = None

        update_backup(backup, properties=["nextRetryDate", "finalRetryDate"])
        logger.info("Updated backup retry info for backup %s, next retry: %s, final retry: %s" %
                    (backup.id, backup.next_retry_date, backup.final_retry_date))

    ####################################################################################################################
    def _compute_final_retry_date(self, backup):
        if backup.plan_occurrence:
            return mid_date_between(backup.plan_occurrence, backup.plan.schedule.next_natural_occurrence())
        else:
            return date_plus_seconds(backup.created_date, 5 * 60 * 60)

    ####################################################################################################################
    def _compute_next_retry_date(self, backup):
        return date_plus_seconds(date_now(), pow(2, backup.try_count - 1) * 60)

    ####################################################################################################################
    def _plan_has_backup_in_progress(self, plan):
        q = {
            "plan._id": plan.id,
            "state": State.IN_PROGRESS
        }
        return get_mbs().backup_collection.find_one(q) is not None

    ####################################################################################################################
