__author__ = 'abdul'

import logging
from .errors import is_exception_retriable, to_mbs_error_code, MBSError
from .backup import Backup
from .date_utils import mid_date_between, date_plus_seconds, date_now

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


########################################################################################################################
def set_task_retry_info(task, task_collection, error):

    error_code = to_mbs_error_code(error)
    # if exception is not retriable then mark backup is not retriable by setting final retry to now
    if not is_exception_retriable(error_code):
        logger.info("Last error for task %s is not retriable. Marking backup is not retriable."
                    " Setting finalRetryDate to task start date %s ..." % (task.id, task.start_date))
        task.final_retry_date = task.start_date
        task.next_retry_date = None
    else:
        # compute final retry date
        if not task.final_retry_date:
            task.final_retry_date = _compute_final_retry_date(task)

        next_retry_date = _compute_next_retry_date(task)
        if next_retry_date <= task.final_retry_date:
            task.next_retry_date = next_retry_date
        else:
            task.next_retry_date = None

    logger.info("Set task retry info for backup %s, next retry: %s, final retry: %s" %
                (task.id, task.next_retry_date, task.final_retry_date))

    task_collection.update_task(task, properties=["nextRetryDate", "finalRetryDate"])
    logger.info("Updated task retry info for backup %s, next retry: %s, final retry: %s" %
                (task.id, task.next_retry_date, task.final_retry_date))

####################################################################################################################
def _compute_final_retry_date(task):
    if isinstance(task, Backup) and task.plan_occurrence:
        return mid_date_between(task.plan_occurrence, task.plan.schedule.next_natural_occurrence())
    else:
        return date_plus_seconds(task.created_date, 5 * 60 * 60)

####################################################################################################################
def _compute_next_retry_date(task):
    return date_plus_seconds(date_now(), pow(2, task.try_count - 1) * 60)