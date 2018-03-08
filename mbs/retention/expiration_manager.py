__author__ = 'abdul'

import traceback
import Queue

from mbs import mbs_logging
from mbs import persistence



from mbs.mbs import get_mbs

from mbs.date_utils import date_now, date_minus_seconds


from mbs.schedule_runner import ScheduleRunner
from mbs.schedule import Schedule
from mbs.globals import State



from mbs.errors import BackupExpirationError

from mbs.utils import document_pretty_string

from mbs.notification.handler import NotificationPriority, NotificationType


###############################################################################
# LOGGER
###############################################################################

logger = mbs_logging.simple_file_logger("BackupExpirationManager", "expiration-manager.log")

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
            get_mbs().notifications.send_notification(subject, message, notification_type=NotificationType.EVENT,
                                                      priority=NotificationPriority.CRITICAL)

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
            get_mbs().notifications.send_notification(subject, message, notification_type=NotificationType.EVENT,
                                                      priority=NotificationPriority.CRITICAL)


        try:
            self._expire_due_onetime_backups()
        except Exception, ex:
            logger.exception("BackupExpirationManager error during onetime backups expiration")
            subject = "BackupExpirationManager Error"
            message = ("BackupExpirationManager Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())
            get_mbs().notifications.send_notification(subject, message, notification_type=NotificationType.EVENT,
                                                      priority=NotificationPriority.CRITICAL)

        try:
            self._expire_due_canceled_backups()
        except Exception, ex:
            logger.exception("BackupExpirationManager error during canceled backups expiration")
            subject = "BackupExpirationManager Error"
            message = ("BackupExpirationManager Error!.\n\nStack Trace:\n%s" %
                       traceback.format_exc())
            get_mbs().notifications.send_notification(subject, message, notification_type=NotificationType.EVENT,
                                                      priority=NotificationPriority.CRITICAL)

        logger.info("BackupExpirationManager: END EXPIRATION CHECK CYCLE")

    ###########################################################################
    def _expire_due_recurring_backups(self):

        total_processed = 0
        total_expired = 0
        total_dont_expire = 0

        logger.info("BackupExpirationManager: Finding all recurring backups"
                    " due for expiration")
        q = self._check_to_expire_query()

        q["plan._id"] = {
            "$exists": True
        }

        s = [("plan._id", -1)]

        logger.info("BackupExpirationManager: Executing query :\n%s" %
                    document_pretty_string(q))

        backups_iter = get_mbs().backup_collection.find_iter(query=q, sort=s, no_cursor_timeout=True)

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
            expirable_backups, non_expirable_backups = self.find_plan_expirable_backups(plan, plan_backups)
            if non_expirable_backups:
                self.mark_plan_backups_not_expirable(plan, non_expirable_backups)
                total_dont_expire += len(non_expirable_backups)

            total_expired += self.expire_plan_dues(plan, expirable_backups)
        except Exception, e:
            logger.exception("BackupExpirationManager Error while"
                             " processing plan '%s'" % plan.id)
            subject = "BackupExpirationManager Error"
            message = ("BackupExpirationManager Error while processing"
                       " plan '%s'\n\nStack Trace:\n%s" %
                       (plan.id, traceback.format_exc()))
            get_mbs().notifications.send_error_notification(subject, message)

        return total_expired, total_dont_expire

    ###########################################################################
    def _expire_due_onetime_backups(self):
        # process onetime backups
        logger.info("BackupExpirationManager: Finding all onetime backups "
                    "due for expiration")

        total_processed = 0
        total_expired = 0
        total_dont_expire = 0
        q = self._check_to_expire_query()

        q["plan._id"] = {
            "$exists": False
        }

        logger.info("BackupExpirationManager: Executing query :\n%s" %
                    document_pretty_string(q))
        onetime_backups_iter = get_mbs().backup_collection.find_iter(query=q, no_cursor_timeout=True)

        for onetime_backup in onetime_backups_iter:
            if self.stop_requested:
                break

            total_processed += 1
            if self.is_onetime_backup_not_expirable(onetime_backup):
                self.mark_backup_never_expire(onetime_backup)
                total_dont_expire += 1
            elif self.is_onetime_backup_due_for_expiration(onetime_backup):
                self.expire_backup(onetime_backup)
                total_expired += 1

        logger.info("BackupExpirationManager: Finished processing Onetime"
                    " Backups.\nTotal Expired=%s, Total Don't Expire=%s, "
                    "Total Processed=%s" %
                    (total_expired, total_dont_expire, total_processed))

    ###########################################################################
    def _expire_due_canceled_backups(self):
        # process onetime backups
        logger.info("BackupExpirationManager: Finding all canceled backups "
                    "due for expiration")

        q = self._check_to_expire_query()

        q["state"] = State.CANCELED
        q["createdDate"] = {
            "$lt": self.expired_canceled_cutoff_date()
        }

        logger.info("BackupExpirationManager: Executing query :\n%s" %
                    document_pretty_string(q))
        canceled_backups_iter = get_mbs().backup_collection.find_iter(query=q, no_cursor_timeout=True)

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
    def is_onetime_backup_due_for_expiration(self, backup):
        return False

    ###########################################################################
    def is_onetime_backup_not_expirable(self, backup):
        return False

    ###########################################################################
    def find_plan_expirable_backups(self, plan, plan_backups):
        return plan_backups, []

    ###########################################################################
    def expire_plan_dues(self, plan, plan_backups):
        dues = self.get_plan_backups_due_for_expiration(plan, plan_backups)

        if dues:
            for due_backup in dues:
                self.expire_backup(due_backup)

        return len(dues) if dues else 0

    ###########################################################################
    def process_plan_retention(self, plan):
        q = self._check_to_expire_query()
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
    def stop(self, blocking=False):
        """
            Override stop to stop queue worker
        """
        super(BackupExpirationManager, self).stop(blocking=blocking)
        self._retention_request_worker.stop(blocking=blocking)

    ###############################################################################
    # QUERY HELPER
    ###############################################################################
    def _check_to_expire_query(self):
        q = {
            "state": State.SUCCEEDED,
            "expiredDate": None,
            "dontExpire": False
        }

        return q

    ###############################################################################
    def mark_plan_backups_not_expirable(self, plan, backups):
        logger.info("Marking following backups for plan '%s' as dontExpire (total of %s)"
                    % (plan.id, len(backups)))

        for backup in backups:
            self.mark_backup_never_expire(backup)

    ###############################################################################
    def mark_backup_never_expire(self, backup):
        logger.info("Mark backup '%s' as not expirable...." % backup.id)

        backup.dont_expire = True
        persistence.update_backup(backup, properties=["dontExpire"],
                                  event_name="MARK_UNEXPIRABLE",
                                  message="Marking as dontExpire")


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
