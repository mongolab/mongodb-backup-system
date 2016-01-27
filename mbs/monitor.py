__author__ = 'abdul'

from schedule_runner import ScheduleRunner
from schedule import Schedule
from globals import State
from mbs import get_mbs
from mbs.notification.handler import NotificationPriority, NotificationType

import logging

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

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

        for backup in get_mbs().backup_collection.find_iter(q):
            if self._backup_system.is_backup_past_due(backup):
                msg = ("You have scheduled backups that has past the maximum "
                       "waiting time" )
                logger.info(msg)
                logger.info("Sending a notification...")
                sbj = "Past due scheduled backups"
                get_mbs().notifications.send_notification(sbj, msg, notification_type=NotificationType.EVENT,
                                                          priority=NotificationPriority.CRITICAL)
                break
