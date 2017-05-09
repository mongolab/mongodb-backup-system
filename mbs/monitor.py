__author__ = 'abdul'

from schedule_runner import ScheduleRunner
from schedule import Schedule
from globals import State
from mbs import get_mbs
from notification.handler import NotificationPriority, NotificationType
from date_utils import date_minus_seconds, date_now
import logging

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# maximum backup wait time in seconds: default to five hours
MAX_BACKUP_WAIT_TIME = 5 * 60 * 60
ONE_OFF_BACKUP_MAX_WAIT_TIME = 60


PAST_DUE_ALERT_SUBJECT = "Past due scheduled backups"

###############################################################################
# BackupMonitor
###############################################################################
class BackupMonitor(ScheduleRunner):
    """
        Backup monitoring thread
    """
    ###########################################################################
    def __init__(self, backup_system):
        self._backup_system = backup_system
        ScheduleRunner.__init__(self, schedule=Schedule(frequency_in_seconds=5*60))
        self._alerting_on_past_due = False

    ###########################################################################
    def run(self):
        super(BackupMonitor, self).run()

    ###########################################################################
    def tick(self):
        self._notify_on_past_due_scheduled_backups()

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

        q = {
            "state": State.SCHEDULED,
        }

        past_due_backup_infos = []

        for backup in get_mbs().backup_collection.find_iter(q, no_cursor_timeout=True):
            if self.is_backup_past_due(backup):
                past_due_backup_infos.append("%s (%s)" % (str(backup.id), backup.source.get_source_info()))

        if past_due_backup_infos:
            msg = ("Backup(s) in SCHEDULED for too long: \n%s" % ", \n".join(past_due_backup_infos))
            logger.info(msg)
            logger.info("Sending a notification...")
            sbj = PAST_DUE_ALERT_SUBJECT
            get_mbs().notifications.send_notification(sbj, msg, notification_type=NotificationType.EVENT,
                                                      priority=NotificationPriority.CRITICAL)
            self._alerting_on_past_due = True

        elif self._alerting_on_past_due:
            self._clear_past_due_alert()

    ###########################################################################
    def _clear_past_due_alert(self):
        self._alerting_on_past_due = False

    ###########################################################################
    def is_backup_past_due(self, backup):
        max_wait_time = self.get_backup_max_wait_time(backup)
        return (backup.state == State.SCHEDULED and
                date_minus_seconds(date_now(), max_wait_time) > backup.get_last_scheduled_date())

    ###########################################################################
    def get_backup_max_wait_time(self, backup):
        max_wait_time = MAX_BACKUP_WAIT_TIME
        if backup.plan:
            if isinstance(backup.plan.schedule, Schedule):
                max_wait_time = min(MAX_BACKUP_WAIT_TIME, backup.plan.schedule.frequency_in_seconds / 2)
        else:
            max_wait_time = ONE_OFF_BACKUP_MAX_WAIT_TIME

        return max_wait_time
