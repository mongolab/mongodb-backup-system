__author__ = 'abdul'

import logging
import persistence
import operator
import traceback

from mbs import get_mbs

from base import MBSObject
from date_utils import date_now, date_minus_seconds


from schedule_runner import ScheduleRunner
from schedule import Schedule
from task import STATE_SUCCEEDED

from task import EVENT_TYPE_ERROR
from target import CloudBlockStorageSnapshotReference


from robustify.robustify import robustify
from errors import raise_if_not_retriable, raise_exception, BackupDeleteError

from utils import document_pretty_string

###############################################################################
# Contains Backup Retention Policies
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

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
    def to_document(self, display_only=False):
        return {
            "_type": "RetainMaxTimePolicy",
            "maxTime": self.max_time
        }

###############################################################################
# BackupExpirationMonitor
###############################################################################

DEFAULT_MONITOR_SCHEDULE = Schedule(frequency_in_seconds=(2 * 60 * 60))


class BackupExpirationMonitor(ScheduleRunner):
    """
        A Thread that periodically expire backups that are due for expiration
    """
    ###########################################################################
    def __init__(self, schedule=None):
        schedule = schedule or DEFAULT_MONITOR_SCHEDULE
        ScheduleRunner.__init__(self, schedule=schedule)
        logger.info("Initializing BackupExpirationMonitor")

    ###########################################################################
    def tick(self):
        try:
            self._expire_backups_due()
        except Exception, ex:
            logger.exception("BackupExpirationMonitor Error")
            subject = "BackupExpirationMonitor Error"
            message = ("BackupExpirationMonitor Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())
            get_mbs().send_error_notification(subject, message, ex)

    ###########################################################################
    def _expire_backups_due(self):

        logger.info("BackupExpirationMonitor: Starting an expiration check "
                    "cycle...")
        total_processed = 0
        total_expired = 0
        total_dont_expire = 0

        logger.info("BackupExpirationMonitor: Finding all recurring backups"
                    " due for expiration")
        q = _check_to_expire_query()

        q["plan._id"] = {
            "$exists": True
        }

        s = [("plan._id", -1)]

        logger.info("BackupExpirationMonitor: Executing query :\n%s" %
                    document_pretty_string(q))

        backups_iter = get_mbs().backup_collection.find_iter(query=q, sort=s)

        current_backup = next(backups_iter, None)

        plan = current_backup and current_backup.plan
        plan_backups = []

        # process all plan backups
        while current_backup:
            total_processed += 1
            if current_backup.plan.id == plan.id:
                plan_backups.append(current_backup)

            current_backup = next(backups_iter, None)

            if not current_backup or current_backup.plan.id != plan.id:
                logger.info("==== Processing plan '%s' .... " % plan.id)
                if self.is_plan_backups_not_expirable(plan):
                    mark_plan_backups_not_expirable(plan, plan_backups)
                    total_dont_expire += len(plan_backups)
                else:
                    total_expired += self.expire_plan_dues(plan, plan_backups)

                plan = current_backup.plan if current_backup else None
                plan_backups = []

        # process onetime backups
        logger.info("BackupExpirationMonitor: Finding all onetime backups "
                    "due for expiration")

        q = _check_to_expire_query()

        q["plan._id"] = {
            "$exists": False
        }

        logger.info("BackupExpirationMonitor: Executing query :\n%s" %
                    document_pretty_string(q))
        onetime_backups_iter = get_mbs().backup_collection.find_iter(query=q)

        for onetime_backup in onetime_backups_iter:
            total_processed += 1
            if self.should_expire_onetime_backup(onetime_backup):
                expire_backup(current_backup)
                total_expired += 1
            elif self.is_backup_not_expirable(onetime_backup):
                mark_backup_never_expire(current_backup)
                total_dont_expire += 1

        logger.info("BackupExpirationMonitor: Finished expiration check cycle. "
                    "Total Expired=%s, Total Don't Expire=%s, "
                    "Total Processed=%s" %
                    (total_expired, total_dont_expire, total_processed))

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
    def is_backup_not_expirable(self, backup):
        return False

    ###########################################################################
    def is_plan_backups_not_expirable(self, plan):
        return False

    ###########################################################################
    def expire_plan_dues(self, plan, plan_backups):
        dues = self.get_plan_backups_due_for_expiration(plan, plan_backups)

        if dues:
            for due_backup in dues:
                expire_backup(due_backup)

        return len(dues) if dues else 0


###############################################################################
# BackupSweeper
###############################################################################

DEFAULT_SWEEP_SCHEDULE = Schedule(frequency_in_seconds=12 * 60 * 60)


class BackupSweeper(ScheduleRunner):
    """
        A Thread that periodically deletes backups targets that
        are due for deletion
    """
    ###########################################################################
    def __init__(self, schedule=None):
        schedule = schedule or DEFAULT_SWEEP_SCHEDULE
        ScheduleRunner.__init__(self, schedule=schedule)
        logger.info("Initializing BackupSweeper")

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
        total_processed = 0
        total_deleted = 0
        total_errored = 0

        logger.info("BackupSweeper: Finding all backups"
                    " due for deletion")
        q = _check_to_delete_query()

        q["plan._id"] = {
            "$exists": True
        }

        s = [("plan._id", -1)]

        logger.info("BackupSweeper: Executing query :\n%s" %
                    document_pretty_string(q))

        backups_iter = get_mbs().backup_collection.find_iter(query=q, sort=s)

        # process all plan backups
        for backup in backups_iter:
            total_processed += 1
            try:
                delete_backup_targets(backup)
                total_deleted += 1
            except Exception, ex:
                logger.exception("BackupSweeper: Error while attempting to "
                                 "delete backup targets for backup '%s'" %
                                 backup.id)
                subject = "BackupSweeper Error"
                message = ("BackupSweeper Error!.\n\nStack Trace:\n%s" %
                            traceback.format_exc())
                get_mbs().send_error_notification(subject, message, ex)
                total_errored += 1

        logger.info("BackupSweeper: Finished sweep cycle. "
                    "Total Deleted=%s, Total Errored=%s, "
                    "Total Processed=%s" %
                    (total_deleted, total_errored, total_processed))

###############################################################################
# QUERY HELPER
###############################################################################
def _check_to_expire_query():
    q = {
        "state": STATE_SUCCEEDED,
        "expiredDate": {"$exists": False},
        "dontExpire": {"$ne": True}
    }

    return q

###############################################################################
def _check_to_delete_query():
    """
        We only delete backups that got expired at least two days ago.
        This is just to make sure that if the expiration monitor screws up we
         would still have time to see what happened
    """
    two_days_ago = date_minus_seconds(date_now(), 2 * 24 * 60 * 60)
    q = {
        "expiredDate": {
            "$lt": two_days_ago
        },
        "deletedDate": {
            "$exists": False
        }
    }

    return q

###############################################################################
# EXPIRE/DELETE BACKUP HELPERS
###############################################################################
def expire_backup(backup):


    # do some validation
    if not backup.target_reference:
        raise BackupDeleteError("Cannot expire backup '%s'. "
                                "Backup never uploaded" % backup.id)

    # validate backups is expirable now if its part of a retained plan
    if backup.plan and backup.plan.retention_policy:
        validate_backup_should_expire_now(backup)

    try:
        backup.expired_date = date_now()
        persistence.update_backup(backup, properties="expiredDate",
                                  event_name="EXPIRING", message="Expiring")

    except Exception, e:
        msg = "Error while attempting to expire backup '%s': " % e
        logger.exception(msg)

###############################################################################
def delete_backup_targets(backup):
    try:
        robustified_delete_backup(backup)
    except Exception, e:
        msg = "Error while attempting to expire backup '%s': " % e
        logger.exception(msg)
        persistence.update_backup(backup, event_name="DELETE_ERROR",
                                  message=msg, event_type=EVENT_TYPE_ERROR)
        # if the backup expiration has errored out for 3 times then mark as
        # unexpirable
        #if backup.event_logged_count("DELETE_ERROR") >= 3:
         #   logger.info("Giving up on delete backup '%s'. Failed at least"
          #              " three times. Marking backup as deleted" %
           #             backup.id)

            #return False
        #else:
        raise


###############################################################################
@robustify(max_attempts=3, retry_interval=5,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def robustified_delete_backup(backup):
    """
        deletes the backup targets
    """

    # do some validation
    if not backup.target_reference:
        raise BackupDeleteError("Cannot delete backup '%s'. "
                                "Backup never uploaded" % backup.id)

    logger.info("Deleting target references for backup '%s'." % backup.id)

    target_ref = backup.target_reference

    logger.info("Deleting primary target reference for backup '%s'." %
                backup.id)
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
def validate_backup_should_expire_now(backup):
    logger.info("Validating if backup '%s' should be expired now" % backup.id)
    rp = backup.plan.retention_policy
    occurrences_to_retain = \
        rp.get_plan_occurrences_to_retain_as_of(backup.plan, date_now())
    if backup.plan_occurrence in occurrences_to_retain:
        raise Exception("Bad attempt to expire backup '%s'. "
                        "Backup must not be expired now." % backup.id)
    else:
        logger.info("Backup '%s' good be expired now" %
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

    target_ref.deleted_date = date_now()
    # if the target reference is a cloud storage one then make the cloud
    # storage object take care of it
    if isinstance(target_ref, CloudBlockStorageSnapshotReference):
        logger.info("Deleting backup '%s' snapshot " % backup.id)
        return target_ref.cloud_block_storage.delete_snapshot(target_ref)
    else:
        logger.info("Deleting backup '%s file" % backup.id)
        return target.delete_file(target_ref)