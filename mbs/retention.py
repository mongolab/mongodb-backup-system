__author__ = 'abdul'

import traceback
import logging
import persistence
import operator

from mbs import get_mbs
from errors import RetentionPolicyError

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

    def apply_policy(self, plan):
        """
            Applies the retention policy by deleting target references for
            expired succeeded backups (e.g. deleting backup files for expired
            backups)
        """
        policy_name = self.__class__.__name__

        for backup in self.get_backups_due(plan):
            try:
                expire_backup(backup)

            except Exception, e:
                logger.error("%s: Error while archiving backup %s. "
                             "Trace: %s" %
                             (policy_name, backup.id, traceback.format_exc()))

                msg = ("Error while applying retention policy on backup %s. " %
                       backup.id)
                raise RetentionPolicyError(msg, cause=e,
                                           details=traceback.format_exc())

    ###########################################################################
    def get_backups_due(self, plan):
        q = _backups_to_check_query(plan_id=plan.id)

        backups = get_mbs().backup_collection.find(query=q)

        return self.filter_backups_due_for_expiration(backups)

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
# BackupSweeper
###############################################################################

DEFAULT_SWEEP_SCHEDULE = Schedule(frequency_in_seconds=20 * 60 * 60)


class BackupSweeper(ScheduleRunner):

    ###########################################################################
    def __init__(self, schedule=None):
        schedule = schedule or DEFAULT_SWEEP_SCHEDULE
        ScheduleRunner.__init__(self, schedule=schedule)
        logger.info("Initializing BackupSweeper")

    ###########################################################################
    def tick(self):
        self._expire_backups_due()

    ###########################################################################
    def _expire_backups_due(self):

        logger.info("BackupSweeper: Starting a sweep cycle...")
        total_processed = 0
        total_expired = 0
        total_dont_expire = 0

        logger.info("BackupSweeper: Finding all recurring backups due for "
                    "expiration")
        q = self.get_backups_to_check_query()

        q["plan._id"] = {
            "$exists": True
        }

        s = [("plan._id", -1)]

        logger.info("BackupSweeper: Executing query :\n%s" %
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
        logger.info("BackupSweeper: Finding all onetime backups due for "
                    "expiration")

        q = self.get_backups_to_check_query()

        q["plan._id"] = {
            "$exists": False
        }

        logger.info("BackupSweeper: Executing query :\n%s" %
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


        logger.info("BackupSweeper: Finished sweeping cycle. Total Expired=%s,"
                    " Total Dont Expire=%s, Total Processed=%s" %
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

    ###########################################################################
    def get_backups_to_check_query(self):
        return _backups_to_check_query()


###############################################################################
# QUERY HELPER
###############################################################################
def _backups_to_check_query(plan_id=None):
    q = {
        "state": STATE_SUCCEEDED,
        "expiredDate": {"$exists": False},
        "dontExpire": {"$ne": True}
    }

    if plan_id:
        q["plan._id"] = plan_id

    return q

###############################################################################
# EXPIRE BACKUP HELPERS
###############################################################################
def expire_backup(backup, expired_date=None):
    try:
        """expired_date = expired_date or date_now()
        return robustified_expire_backup(backup, expired_date)
        """
        return False
    except Exception, e:
        msg = "Error while attempting to expire backup '%s': " % e
        logger.exception(msg)
        persistence.update_backup(backup, event_name="EXPIRE_ERROR",
                                  message=msg, event_type=EVENT_TYPE_ERROR)
        # if the backup expiration has errored out for 3 times then mark as
        # unexpirable
        if backup.event_logged_count("EXPIRE_ERROR") >= 3:
            logger.info("Giving up on expire backup '%s'. Failed at least"
                        " three times. Need to mark as unexpirable" %
                        backup.id)
            mark_backup_never_expire(backup)
            return False
        else:
            raise


###############################################################################
@robustify(max_attempts=3, retry_interval=5,
           do_on_exception=raise_if_not_retriable,
           do_on_failure=raise_exception)
def robustified_expire_backup(backup, expired_date):
    """
        expires the backup
    """

    # do some validation
    if not backup.target_reference:
        raise BackupDeleteError("Cannot expire backup '%s'. "
                                "Backup never uploaded" % backup.id)

    # validate if the backup has been expired already
    if backup.expired_date:
        logger.warning("expire_backup(): Backup '%s' is already expired."
                       " Ignoring..." % backup.id)
        return

    logger.info("Expiring backup '%s', expired date: '%s'." %
                (backup.id, expired_date))

    target_ref = backup.target_reference

    # if the target reference is a cloud storage one then make the cloud
    # storage object take care of it
    exists = do_delete_target_ref(backup, target_ref)

    # expire log file
    if backup.log_target_reference:
        exists = do_delete_target_ref(backup, backup.log_target_reference)

    # set expired date
    backup.expired_date = expired_date
    persistence.update_backup(backup, properties=["expiredDate"],
                              event_name="EXPIRING", message="Expiring")

    logger.info("Backup %s expired successfully!" % backup.id)
    return exists

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
def do_delete_target_ref(backup, target_ref):
    if isinstance(target_ref, CloudBlockStorageSnapshotReference):
        logger.info("Deleting backup '%s' snapshot " % backup.id)
        return target_ref.cloud_block_storage.delete_snapshot(target_ref)
    else:
        logger.info("Deleting backup '%s file" % backup.id)
        return backup.target.delete_file(target_ref)