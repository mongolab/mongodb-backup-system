__author__ = 'abdul'

from schedule_runner import ScheduleRunner
from schedule import Schedule
from globals import State
from mbs import get_mbs
from date_utils import date_now, date_minus_seconds
from task import EVENT_STATE_CHANGE
from events import EventListener, BackupEventTypes

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
        self._backup_monitor_event_listener = BackupMonitorEventListener(backup_monitor=self)

    ###########################################################################
    def run(self):
        # register event listener with event queue
        get_mbs().event_queue.register_event_listener(self._backup_monitor_event_listener)
        super(BackupMonitor, self).run()

    ###########################################################################
    def tick(self):
        self._notify_on_past_due_scheduled_backups()

    ###########################################################################
    def on_backup_event(self, event):
        logger.info("BACKUP %s finished with state %s" % (event.backup.id, event.state))

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
                get_mbs().send_notification(sbj, msg)
                break

###############################################################################
# BackupMonitorEventListener
###############################################################################
class BackupMonitorEventListener(EventListener):

    ###########################################################################
    def __init__(self, backup_monitor=None):
        super(BackupMonitorEventListener, self).__init__()
        self.event_types = [BackupEventTypes.BACKUP_FINISHED]
        self.name = "BackupMonitorEventListener"
        self._backup_monitor = backup_monitor

    ###########################################################################
    def handle_event(self, event):
        self._backup_monitor.on_backup_event(event)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(BackupMonitorEventListener, self).to_document(display_only=display_only)
        doc.update({
            "_type": "BackupMonitorEventListener"
        })

        return doc
