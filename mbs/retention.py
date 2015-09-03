__author__ = 'abdul'

import mbs_logging
import persistence
import operator
import traceback
import Queue
from mbs import get_mbs

from base import MBSObject
from date_utils import date_now, date_minus_seconds, date_plus_seconds


from schedule_runner import ScheduleRunner
from schedule import Schedule
from globals import State, EventType

from target import CloudBlockStorageSnapshotReference


from robustify.robustify import robustify
from errors import (
    raise_if_not_retriable, raise_exception, TargetInaccessibleError,
    BackupExpirationError, BackupSweepError)

from utils import document_pretty_string

from threading import Thread
import time

###############################################################################
# Contains Backup Retention Policies
###############################################################################
logger = mbs_logging.simple_file_logger("Retention", "retention.log")

###############################################################################
# RetentionPolicy
###############################################################################
class RetentionPolicy(MBSObject):

    ###########################################################################
    def __init__(self):
        MBSObject.__init__(self)

    ###########################################################################
    def filter_backups_due_for_expiration(self, backups):
        """
            Returns a list of backups that should expired and should be
            removed. Should be overridden by sub classes
        """
        return []

    ###########################################################################
    def get_plan_occurrences_to_retain_as_of(self, plan, dt):
        pass

    ###########################################################################
    def get_occurrence_expected_expire_date(self, plan, occurrence):
        pass

###############################################################################
# RetainLastNPolicy
###############################################################################
class RetainLastNPolicy(RetentionPolicy):
    """
        Retains the last 'n' backups
    """
    ###########################################################################
    def __init__(self, retain_count=5):
        RetentionPolicy.__init__(self)
        self._retain_count = retain_count

    ###########################################################################
    @property
    def retain_count(self):
        return self._retain_count

    @retain_count.setter
    def retain_count(self, retain_count):
        self._retain_count = retain_count

    ###########################################################################
    def filter_backups_due_for_expiration(self, backups):

        backups.sort(key=operator.attrgetter('created_date'), reverse=True)

        if len(backups) <= self.retain_count:
            return []
        else:
            return backups[self.retain_count:]

    ###########################################################################
    def get_plan_occurrences_to_retain_as_of(self, plan, dt):
        return plan.schedule.last_n_occurrences(self.retain_count, dt=dt)

    ###########################################################################
    def get_occurrence_expected_expire_date(self, plan, occurrence):
        # get n occurrences to keep as of this occurrence and return the
        # last one ;)
        dt = date_plus_seconds(occurrence, 1)
        ocs = plan.schedule.next_n_occurrences(self.retain_count,
                                               dt=occurrence)
        return ocs[-1]

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "RetainLastNPolicy",
            "retainCount": self.retain_count
        }


###############################################################################
# RetainTimePolicy
###############################################################################
class RetainMaxTimePolicy(RetentionPolicy):
    """
        Retains T time worth of data. i.e. Backup date is within now() - T
    """
    ###########################################################################
    def __init__(self, max_time=0):
        RetentionPolicy.__init__(self)
        self._max_time = max_time

    ###########################################################################
    @property
    def max_time(self):
        return self._max_time

    @max_time.setter
    def max_time(self, max_time):
        self._max_time = max_time

    ###########################################################################
    def filter_backups_due_for_expiration(self, backups):

        earliest_date_to_keep = date_minus_seconds(date_now(), self.max_time)

        return filter(lambda backup:
                      backup.created_date < earliest_date_to_keep,
                      backups)

    ###########################################################################
    def get_plan_occurrences_to_retain_as_of(self, plan, dt):
        end_date = dt
        start_date = date_minus_seconds(end_date, self.max_time)
        return plan.schedule.natural_occurrences_between(start_date, end_date)

    ###########################################################################
    def get_occurrence_expected_expire_date(self, plan, occurrence):
        return date_plus_seconds(occurrence, self.max_time)

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "RetainMaxTimePolicy",
            "maxTime": self.max_time
        }

###############################################################################
# BackupExpirationManager
###############################################################################

DEFAULT_EXP_SCHEDULE = Schedule(frequency_in_seconds=(5 * 60 * 60))

DEFAULT_EXP_CANCELED_DELAY = 5 * 60 * 60 * 24

class BackupExpirationManager(ScheduleRunner):
    """
        A Thread that periodically expire backups that are due for expiration
    """
    ###########################################################################
    def __init__(self, schedule=None):
        schedule = schedule or DEFAULT_EXP_SCHEDULE
        ScheduleRunner.__init__(self, schedule=schedule)
        self._test_mode = False
        self._retention_request_queue = Queue.Queue()
        self._retention_request_worker = BackupRetentionRequestWorker(
            self, self._retention_request_queue)

        self._expire_canceled_delay_in_seconds = DEFAULT_EXP_CANCELED_DELAY

    ###########################################################################
    def start(self):
        super(BackupExpirationManager, self).start()
        self._retention_request_worker.start()

    ###########################################################################
    @property
    def test_mode(self):
        return self._test_mode

    @test_mode.setter
    def test_mode(self, val):
        self._test_mode = val



    ###########################################################################
    @property
    def test_mode(self):
        return self._test_mode

    @test_mode.setter
    def test_mode(self, val):
        self._test_mode = val

    ###########################################################################
    @property
    def expire_canceled_delay_in_seconds(self):
        return self._expire_canceled_delay_in_seconds

    @expire_canceled_delay_in_seconds.setter
    def expire_canceled_delay_in_seconds(self, val):
        self._expire_canceled_delay_in_seconds = val

    ###########################################################################
    def tick(self):
        try:
            self._expire_backups_due()
        except Exception, ex:
            logger.exception("BackupExpirationManager Error")
            subject = "BackupExpirationManager Error"
            message = ("BackupExpirationManager Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())
            get_mbs().send_error_notification(subject, message, ex)

    ###########################################################################
    def _expire_backups_due(self):
        logger.info("BackupExpirationManager: START EXPIRATION CHECK CYCLE")

        # expire recurring backups
        try:
            self._expire_due_recurring_backups()
        except Exception, ex:
            logger.exception("BackupExpirationManager error during recurring backups expiration")
            subject = "BackupExpirationManager Error"
            message = ("BackupExpirationManager Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())
            get_mbs().send_error_notification(subject, message, ex)


        try:
            self._expire_due_onetime_backups()
        except Exception, ex:
            logger.exception("BackupExpirationManager error during onetime backups expiration")
            subject = "BackupExpirationManager Error"
            message = ("BackupExpirationManager Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())
            get_mbs().send_error_notification(subject, message, ex)

        try:
            self._expire_due_canceled_backups()
        except Exception, ex:
            logger.exception("BackupExpirationManager error during canceled backups expiration")
            subject = "BackupExpirationManager Error"
            message = ("BackupExpirationManager Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())
            get_mbs().send_error_notification(subject, message, ex)

        logger.info("BackupExpirationManager: END EXPIRATION CHECK CYCLE")

    ###########################################################################
    def _expire_due_recurring_backups(self):

        total_processed = 0
        total_expired = 0
        total_dont_expire = 0

        logger.info("BackupExpirationManager: Finding all recurring backups"
                    " due for expiration")
        q = _check_to_expire_query()

        q["plan._id"] = {
            "$exists": True
        }

        s = [("plan._id", -1)]

        logger.info("BackupExpirationManager: Executing query :\n%s" %
                    document_pretty_string(q))

        backups_iter = get_mbs().backup_collection.find_iter(query=q, sort=s,
                                                             timeout=False)

        current_backup = next(backups_iter, None)

        plan = current_backup.plan if current_backup else None
        plan_backups = []

        # process all plan backups
        while current_backup and not self.stop_requested:
            total_processed += 1

            if current_backup.plan.id == plan.id:
                plan_backups.append(current_backup)

            current_backup = next(backups_iter, None)
            # process the current plan
            if not current_backup or current_backup.plan.id != plan.id:
                plan_total_expired, plan_total_dont_expire = \
                    self._process_plan(plan, plan_backups)
                total_expired += plan_total_expired
                total_dont_expire = plan_total_dont_expire

                plan = current_backup.plan if current_backup else None
                plan_backups = []

        logger.info("BackupExpirationManager: Finished processing Recurring "
                    "Backups.\nTotal Expired=%s, Total Don't Expire=%s, "
                    "Total Processed=%s" %
                    (total_expired, total_dont_expire, total_processed))

    ###########################################################################
    def _process_plan(self, plan, plan_backups):
        total_dont_expire = 0
        total_expired = 0
        logger.info("==== Processing plan '%s' .... " % plan.id)
        # Ensure we have the latest revision of the backup plan
        plan = persistence.get_backup_plan(plan.id) or plan
        try:
            if self.is_plan_backups_not_expirable(plan):
                mark_plan_backups_not_expirable(plan, plan_backups)
                total_dont_expire += len(plan_backups)
            else:
                total_expired += self.expire_plan_dues(plan,
                                                       plan_backups)
        except Exception, e:
            logger.exception("BackupExpirationManager Error while"
                             " processing plan '%s'" % plan.id)
            subject = "BackupExpirationManager Error"
            message = ("BackupExpirationManager Error while processing"
                       " plan '%s'\n\nStack Trace:\n%s" %
                       (plan.id, traceback.format_exc()))
            get_mbs().send_error_notification(subject, message, e)

        return total_expired, total_dont_expire

    ###########################################################################
    def _expire_due_onetime_backups(self):
        # process onetime backups
        logger.info("BackupExpirationManager: Finding all onetime backups "
                    "due for expiration")

        total_processed = 0
        total_expired = 0
        total_dont_expire = 0
        q = _check_to_expire_query()

        q["plan._id"] = {
            "$exists": False
        }

        logger.info("BackupExpirationManager: Executing query :\n%s" %
                    document_pretty_string(q))
        onetime_backups_iter = get_mbs().backup_collection.find_iter(
            query=q, timeout=False)

        for onetime_backup in onetime_backups_iter:
            if self.stop_requested:
                break

            total_processed += 1
            if self.should_expire_onetime_backup(onetime_backup):
                self.expire_backup(onetime_backup)
                total_expired += 1
            elif self.is_onetime_backup_not_expirable(onetime_backup):
                mark_backup_never_expire(onetime_backup)
                total_dont_expire += 1

        logger.info("BackupExpirationManager: Finished processing Onetime"
                    " Backups.\nTotal Expired=%s, Total Don't Expire=%s, "
                    "Total Processed=%s" %
                    (total_expired, total_dont_expire, total_processed))

    ###########################################################################
    def _expire_due_canceled_backups(self):
        # process onetime backups
        logger.info("BackupExpirationManager: Finding all canceled backups "
                    "due for expiration")

        q = _check_to_expire_query()

        q["state"] = State.CANCELED
        q["createdDate"] = {
            "$lt": self.expired_canceled_cutoff_date()
        }

        logger.info("BackupExpirationManager: Executing query :\n%s" %
                    document_pretty_string(q))
        canceled_backups_iter = get_mbs().backup_collection.find_iter(
            query=q, timeout=False)

        for backup in canceled_backups_iter:
            if self.stop_requested:
                break
            # for canceled backups, we always expire them immediately
            self.expire_backup(backup)

        logger.info("BackupExpirationManager: Finished processing canceled"
                    " Backups")

    ###########################################################################
    def get_plan_backups_due_for_expiration(self, plan, plan_backups):
        rp = plan.retention_policy
        if rp and self.is_plan_backups_expirable(plan):
            return rp.filter_backups_due_for_expiration(plan_backups)

    ###########################################################################
    def is_plan_backups_expirable(self, plan):
        # We only allow expiring backups that has a whose plans still exist
        #  and has a retention policy
        return persistence.get_backup_plan(plan.id) is not None

    ###########################################################################
    def should_expire_onetime_backup(self, backup):
        return False

    ###########################################################################
    def is_onetime_backup_not_expirable(self, backup):
        return False

    ###########################################################################
    def is_plan_backups_not_expirable(self, plan):
        return False

    ###########################################################################
    def expire_plan_dues(self, plan, plan_backups):
        dues = self.get_plan_backups_due_for_expiration(plan, plan_backups)

        if dues:
            for due_backup in dues:
                self.expire_backup(due_backup)

        return len(dues) if dues else 0

    ###########################################################################
    def process_plan_retention(self, plan):
        q = _check_to_expire_query()
        q["plan._id"] = plan.id

        plan_backups = get_mbs().backup_collection.find(q)

        self._process_plan(plan, plan_backups)

    ###########################################################################
    def expire_backup(self, backup, force=False):
        # do some validation
        if backup.state == State.SUCCEEDED and not backup.target_reference:
            raise BackupExpirationError("Cannot expire backup '%s'. "
                                        "Backup never uploaded" % backup.id)

        if not(force or backup.state == State.CANCELED):
            self.validate_backup_expiration(backup)

        if not self.test_mode:
            try:
                logger.info("BackupExpirationManager: Expiring backup '%s'" %
                            backup.id)
                backup.expired_date = date_now()
                persistence.update_backup(backup, properties="expiredDate",
                                          event_name="EXPIRING",
                                          message="Expiring")

            except Exception, e:
                msg = "Error while attempting to expire backup '%s': " % e
                logger.exception(msg)
        else:
            logger.info("BackupExpirationManager: NOOP. Test mode enabled. "
                        "Not expiring backup '%s'" %
                        backup.id)
            return

    ###########################################################################
    def validate_backup_expiration(self, backup):
        logger.info("Validating backup '%s' expiration. startDate='%s',"
                    " endDate='%s'" % (backup.id, backup.start_date,
                                       backup.end_date))
        # recurring backup validation
        if backup.plan:
                self.validate_recurring_backup_expiration(backup)
        else:
            self.validate_onetime_backup_expiration(backup)

    ###########################################################################
    def validate_recurring_backup_expiration(self, backup):
        logger.info("Validating if recurring backup '%s' should be "
                    "expired now" % backup.id)
        # Ensure we have the latest revision of the backup plan when possible
        plan = persistence.get_backup_plan(backup.plan.id) or backup.plan

        rp = plan.retention_policy

        if not rp:
            raise BackupExpirationError(
                "Bad attempt to expire backup '%s'. "
                "Backup plan does not have a retention policy" % backup.id)
        occurrences_to_retain = \
            rp.get_plan_occurrences_to_retain_as_of(plan, date_now())

        if backup.plan_occurrence in occurrences_to_retain:
            raise BackupExpirationError(
                "Bad attempt to expire backup '%s'. Backup must not be"
                " expired now." % backup.id)
        else:
            logger.info("Backup '%s' good be expired now" %
                        backup.id)

    ###########################################################################
    def validate_onetime_backup_expiration(self, backup):
        """
            To be overridden
        """
        logger.info("Validating if onetime backup '%s' should be expired now" %
                    backup.id)

    ###########################################################################
    def request_plan_retention(self, plan):
        self._retention_request_queue.put(plan)


    ###########################################################################
    def expired_canceled_cutoff_date(self):
        return date_minus_seconds(date_now(),
                                  self.expire_canceled_delay_in_seconds)

    ###########################################################################
    def stop(self):
        """
            Override stop to stop queue worker
        """
        super(BackupExpirationManager, self).stop()
        self._retention_request_worker.stop()

###############################################################################
RETENTION_WORKER_SCHEDULE = Schedule(frequency_in_seconds=30)

class BackupRetentionRequestWorker(ScheduleRunner):
    """
        A Thread that periodically expire backups that are due for expiration
    """
    ###########################################################################
    def __init__(self, expiration_manager, queue):
        ScheduleRunner.__init__(self, schedule=RETENTION_WORKER_SCHEDULE)
        self._expiration_manager = expiration_manager
        self._queue = queue

    ###########################################################################
    def tick(self):

        while not self._queue.empty():
            plan = self._queue.get()
            self._expiration_manager.process_plan_retention(plan)

    ###########################################################################

###############################################################################
# BackupSweeper
###############################################################################

DEFAULT_SWEEP_SCHEDULE = Schedule(frequency_in_seconds=12 * 60 * 60)
DEFAULT_DELETE_DELAY_IN_SECONDS = 5 * 24 * 60 * 60  # 5 days
SWEEP_WORKER_COUNT = 10

class BackupSweeper(ScheduleRunner):
    """
        A Thread that periodically deletes backups targets that
        are due for deletion
    """
    ###########################################################################
    def __init__(self, schedule=None):
        schedule = schedule or DEFAULT_SWEEP_SCHEDULE
        ScheduleRunner.__init__(self, schedule=schedule)
        self._test_mode = False
        self._delete_delay_in_seconds = DEFAULT_DELETE_DELAY_IN_SECONDS
        self._sweep_workers = None
        self._sweep_queue = Queue.Queue()

        # cycle stats

        self._cycle_total_processed = 0
        self._cycle_total_deleted = 0
        self._cycle_total_errored = 0

    ###########################################################################
    @property
    def test_mode(self):
        return self._test_mode

    @test_mode.setter
    def test_mode(self, val):
        self._test_mode = val

    ###########################################################################
    @property
    def delete_delay_in_seconds(self):
        return self._delete_delay_in_seconds

    @delete_delay_in_seconds.setter
    def delete_delay_in_seconds(self, val):
        self._delete_delay_in_seconds = val

    ###########################################################################
    def tick(self):
        try:
            self._delete_backups_targets_due()
        except Exception, ex:
            logger.exception("BackupSweeper Error")
            subject = "BackupSweeper Error"
            message = ("BackupSweeper Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())
            get_mbs().send_error_notification(subject, message, ex)

    ###########################################################################
    def _delete_backups_targets_due(self):

        logger.info("BackupSweeper: Starting a sweep cycle...")

        # clear stats
        self._cycle_total_processed = 0
        self._cycle_total_errored = 0
        self._cycle_total_deleted = 0
        self._sweep_workers = []

        self._start_workers()

        if self.test_mode:
            logger.info("BackupSweeper: Running in TEST MODE. Nothing will"
                        " be really deleted")

        logger.info("BackupSweeper: Finding all backups"
                    " due for deletion")
        q = self._check_to_delete_query()

        logger.info("BackupSweeper: Executing query :\n%s" %
                    document_pretty_string(q))

        backups_iter = get_mbs().backup_collection.find_iter(query=q,
                                                             timeout=False)

        # process all plan backups
        for backup in backups_iter:
            if self.stop_requested:
                break

            self._sweep_queue.put(backup)

        self._finish_cycle()


        logger.info("BackupSweeper: Finished sweep cycle. "
                    "Total Deleted=%s, Total Errored=%s, "
                    "Total Processed=%s" %
                    (self._cycle_total_deleted,
                    self._cycle_total_errored,
                    self._cycle_total_processed))

    ###########################################################################
    def _start_workers(self):
        for i in range(0, SWEEP_WORKER_COUNT):
            sweep_worker = SweepWorker(self, self._sweep_queue)
            self._sweep_workers.append(sweep_worker)
            sweep_worker.start()

    ###########################################################################
    def _finish_cycle(self):
        self._wait_for_queue_to_be_empty()
        self._stop_and_wait_for_all_workers_to_finish()

    ###########################################################################
    def _wait_for_queue_to_be_empty(self):
        while not self._sweep_queue.empty():
            time.sleep(1)

    ###########################################################################
    def _stop_and_wait_for_all_workers_to_finish(self):
        # request stop
        for worker in self._sweep_workers:
            worker.stop()

        # join and gather stats
        for worker in self._sweep_workers:
            worker.join()
            self._cycle_total_processed += worker.total_processed
            self._cycle_total_deleted += worker.total_deleted
            self._cycle_total_errored += worker.total_errored

    ###########################################################################
    def _check_to_delete_query(self):
        """
            We only delete backups that got expired at least two days ago.
            This is just to make sure that if the expiration monitor screws up we
             would still have time to see what happened
        """
        q = {
            "expiredDate": {
                "$lt": self.max_expire_date_to_delete()
            },
            "deletedDate": None
        }

        return q

    ###########################################################################
    def delete_backup_targets(self, backup):
        logger.info("Attempt to delete targets for backup '%s'" % backup.id)
        self.validate_backup_target_delete(backup)
        try:
            if not self.test_mode:
                robustified_delete_backup(backup)
                return True
            else:
                logger.info("NOOP. Running in test mode. Not deleting "
                            "targets for backup '%s'" % backup.id)
        except Exception, e:
            msg = "Error while attempting to expire backup '%s': " % e
            logger.exception(msg)
            persistence.update_backup(backup,
                                      event_name="DELETE_ERROR",
                                      message=msg,
                                      event_type=EventType.ERROR)
            # if the backup expiration has errored out for 3 times then mark as
            # unexpirable
            if backup.event_logged_count("DELETE_ERROR") >= 3:
                logger.info("Giving up on delete backup '%s'. Failed at least"
                            " three times. Marking backup as deleted" %
                            backup.id)
                raise

    ###########################################################################
    def validate_backup_target_delete(self, backup):
        logger.info("Validating delete of backup '%s'. startDate='%s',"
                    " expiredDate='%s' ..." % (backup.id, backup.start_date,
                                           backup.expired_date))
        if not backup.expired_date:
            raise BackupSweepError(
                "Bad target delete attempt for backup '%s'. Backup has "
                "not expired yet" % backup.id)

        cutoff_date = self.max_expire_date_to_delete()
        if backup.expired_date > cutoff_date:
            msg = ("Bad target delete attempt for backup '%s'. Backup expired"
                   " date '%s' is not before  max expire date to delete '%s'" %
                   (backup.id, backup.expired_date, cutoff_date))
            raise BackupSweepError(msg)

        logger.info("Validation succeeded. Backup '%s' good to be deleted" %
                    backup.id)

    ###########################################################################
    def max_expire_date_to_delete(self):
        return date_minus_seconds(date_now(), self.delete_delay_in_seconds)

###############################################################################

SWEEP_WORKER_SCHEDULE = Schedule(frequency_in_seconds=5)

class SweepWorker(ScheduleRunner):
    """
        A Thread that periodically expire backups that are due for expiration
    """
    ###########################################################################
    def __init__(self, backup_sweeper, sweep_queue):
        ScheduleRunner.__init__(self, schedule=SWEEP_WORKER_SCHEDULE)
        self._backup_sweeper = backup_sweeper
        self._sweep_queue = sweep_queue
        self._total_processed = 0
        self._total_deleted = 0
        self._total_errored = 0


    ###########################################################################
    @property
    def total_processed(self):
        return self._total_processed

    ###########################################################################
    @property
    def total_deleted(self):
        return self._total_deleted

    ###########################################################################
    @property
    def total_errored(self):
        return self._total_errored

    ###########################################################################
    def tick(self):
        while not self._sweep_queue.empty():
            backup = self._sweep_queue.get(True)
            self._total_processed += 1
            try:
                deleted = self._backup_sweeper.delete_backup_targets(backup)
                if deleted:
                    self._total_deleted += 1
            except Exception, ex:
                self._total_errored += 1
                msg = ("BackupSweeper: Error while attempting to "
                       "delete backup targets for backup '%s'" % backup.id)
                logger.exception(msg)
                subject = "BackupSweeper Error"
                msg = ("%s\n\nStack Trace:\n%s" % (msg,
                                                   traceback.format_exc()))
                get_mbs().send_error_notification(subject, msg, ex)


###############################################################################
# QUERY HELPER
###############################################################################
def _check_to_expire_query():
    q = {
        "state": State.SUCCEEDED,
        "expiredDate": None,
        "dontExpire": False
    }

    return q

###############################################################################
# EXPIRE/DELETE BACKUP HELPERS
###############################################################################
@robustify(max_attempts=3, retry_interval=5,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def robustified_delete_backup(backup):
    """
        deletes the backup targets
    """
    # do some validation,
    target_ref = backup.target_reference

    if backup.state == State.SUCCEEDED and not target_ref:
        raise BackupSweepError("Cannot delete backup '%s'. "
                               "Backup never uploaded" % backup.id)

    logger.info("Deleting target references for backup '%s'." % backup.id)



    logger.info("Deleting primary target reference for backup '%s'." %
                backup.id)
    # target ref can be None for CANCELED backups
    if target_ref:
        do_delete_target_ref(backup, backup.target, target_ref)

    # delete log file
    if backup.log_target_reference:
        logger.info("Deleting log target reference for backup '%s'." %
                    backup.id)
        do_delete_target_ref(backup, backup.target,
                             backup.log_target_reference)

    if backup.secondary_target_references:
        logger.info("Deleting secondary target references for backup '%s'." %
                    backup.id)
        sec_targets = backup.secondary_targets
        sec_target_refs = backup.secondary_target_references
        for (sec_target, sec_tgt_ref) in zip(sec_targets, sec_target_refs):
            logger.info("Deleting secondary target reference %s for backup "
                        "'%s'." % (sec_tgt_ref, backup.id))
            do_delete_target_ref(backup, sec_target, sec_tgt_ref)

    # set deleted date
    backup.deleted_date = date_now()
    update_props = ["deletedDate", "targetReference",
                    "secondaryTargetReferences"]
    persistence.update_backup(backup, properties=update_props,
                              event_name="DELETING",
                              message="Deleting target references")

    logger.info("Backup %s target references deleted successfully!" %
                backup.id)

###############################################################################
def mark_plan_backups_not_expirable(plan, backups):
    logger.info("Marking all backups for plan '%s' as dontExpire (total of %s)"
                % (plan.id, len(backups)))

    for backup in backups:
        mark_backup_never_expire(backup)

###############################################################################
def mark_backup_never_expire(backup):
    logger.info("Mark backup '%s' as not expirable...." % backup.id)

    backup.dont_expire = True
    persistence.update_backup(backup, properties=["dontExpire"],
                              event_name="MARK_UNEXPIRABLE",
                              message="Marking as dontExpire")

###############################################################################
def do_delete_target_ref(backup, target, target_ref):

    if target_ref.preserve:
        logger.info("Skipping deletion for target ref %s (backup '%s') because"
                    " it is preserved" % (target_ref, backup.id))
        return
    try:
        target_ref.deleted_date = date_now()
        # if the target reference is a cloud storage one then make the cloud
        # storage object take care of it
        if isinstance(target_ref, CloudBlockStorageSnapshotReference):
            logger.info("Deleting backup '%s' snapshot " % backup.id)
            return target_ref.cloud_block_storage.delete_snapshot(target_ref)
        else:
            logger.info("Deleting backup '%s file" % backup.id)
            return target.delete_file(target_ref)
    except TargetInaccessibleError as e:
        msg = "Target %s for backup %s is no longer accessible.\n%s" % (
            target, backup.id, e.message
        )
        logger.warn(msg)
        persistence.update_backup(backup,
                                  event_name="DELETE_ERROR",
                                  message=msg,
                                  event_type=EventType.WARNING)
        return False

###############################################################################
def get_expiration_manager():
    return get_mbs().backup_system.backup_expiration_manager

