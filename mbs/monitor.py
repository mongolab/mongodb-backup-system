__author__ = 'abdul'

from schedule_runner import ScheduleRunner
from schedule import Schedule
from globals import State
from mbs import get_mbs
from date_utils import date_now, date_minus_seconds
from task import EVENT_STATE_CHANGE

import logging

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

########################################################################################################################
MONITOR_SCHEDULE = Schedule(frequency_in_seconds=10*60)

RESCHEDULE_PERIOD = 30 * 60

########################################################################################################################
class BackupMonitor(ScheduleRunner):
    """
        Backup monitoring thread
    """
    ####################################################################################################################
    def __init__(self, backup_system):
        self._backup_system = backup_system
        ScheduleRunner.__init__(self, schedule=MONITOR_SCHEDULE)

    ####################################################################################################################
    def tick(self):

        self._notify_on_past_due_scheduled_backups()
        self._cancel_past_cycle_backups()
        self._reschedule_in_cycle_failed_backups()


    ####################################################################################################################
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
            logger.info("Cancelling backup %s" % backup._id)
            backup.state = State.CANCELED
            bc.update_task(backup, properties="state",
                           event_name=EVENT_STATE_CHANGE,
                           message="Backup is past due. Canceling...")

    ####################################################################################################################
    def _reschedule_in_cycle_failed_backups(self):
        """
        Reschedule failed reschedulable backups that failed at least
        RESCHEDULE_PERIOD seconds ago
        """

        q = {
            "state": State.FAILED,
            "reschedulable": True
        }

        cuttoff_date = date_minus_seconds(date_now(), RESCHEDULE_PERIOD)
        for backup in get_mbs().backup_collection.find(q):
            if not backup.end_date or backup.end_date <= cuttoff_date:
                self._backup_system.reschedule_backup(backup)
