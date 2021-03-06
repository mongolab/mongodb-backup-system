__author__ = 'abdul'

import logging
from .errors import is_exception_retriable, NonRetriableError
from .backup import Backup
from .restore import Restore
from .date_utils import mid_date_between, date_plus_seconds, date_now
from .mbs import get_mbs
from .globals import State
from .events import BackupFinishedEvent, RestoreFinishedEvent
from .notification.handler import NotificationPriority

import traceback

########################################################################################################################
# LOGGER
########################################################################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

########################################################################################################################

MAX_ONEOFF_ATTEMPTS = 3

########################################################################################################################
def trigger_task_finished_event(task, state):

    # NOOP when there is no event queue
    if not get_mbs().event_queue:
        return

    if isinstance(task, Backup):
        finished_event = BackupFinishedEvent(backup=task, state=state)
        task_type = "Backup"
    elif isinstance(task, Restore):
        finished_event = RestoreFinishedEvent(restore=task, state=state)
        task_type = "Restore"
    else:
        raise Exception("Unknown task type!!!!")
    try:
        get_mbs().event_queue.create_event(finished_event)
        logger.info("Event for %s %s created successfully!" % (task_type, task.id))
    except Exception, ex:
        logger.exception("Failed to trigger finished event for %s %s" % (task_type, task.id))
        # notify on failures to trigger task event
        sbj = "Failed to trigger Finished Event"
        msg = "Failed to trigger Finished Event: \nError: %s" % traceback.format_exc()
        priority = NotificationPriority.CRITICAL if state == State.FAILED else NotificationPriority.NORMAL
        get_mbs().notifications.send_event_notification(sbj, msg, priority=priority)

########################################################################################################################
def set_task_retry_info(task, error):

    if not isinstance(task, Backup):
        # NOOP on restores
        return

    # Non retriable errors
    if isinstance(error, NonRetriableError):
        task.final_retry_date = task.start_date
        task.next_retry_date = None
        return


    # compute final retry date
    if not task.final_retry_date:
        task.final_retry_date = _compute_final_retry_date(task)

    next_retry_date = _compute_next_retry_date(task, error)
    if not task.final_retry_date or next_retry_date <= task.final_retry_date:
        task.next_retry_date = next_retry_date
    elif task.final_retry_date > date_now():
        task.next_retry_date = task.final_retry_date
    else:
        task.next_retry_date = None

    logger.info("Set task retry info for backup %s, next retry: %s, final retry: %s" %
                (task.id, task.next_retry_date, task.final_retry_date))


########################################################################################################################
def _compute_final_retry_date(task):
    if isinstance(task, Backup):
        if task.plan_occurrence:
            return mid_date_between(task.plan_occurrence, task.plan.schedule.next_natural_occurrence())
        elif task.try_count >= MAX_ONEOFF_ATTEMPTS: # oneoff final retry date is the last attempts start date
            return task.start_date
    else:
        # restore, NOOP
        pass

########################################################################################################################
def _compute_next_retry_date(task, error):
    if task.try_count == 1 and not is_exception_retriable(error):
        initial_backoff = 60 * 60
    else:
        initial_backoff = 0

    backoff = initial_backoff + pow(2, task.try_count - 1) * 60

    return date_plus_seconds(date_now(), backoff)