__author__ = 'abdul'

import traceback
import multiprocessing
from mbs import mbs_logging
from mbs import persistence




from mbs.mbs import get_mbs

from mbs.date_utils import date_now, date_minus_seconds


from mbs.schedule_runner import ScheduleRunner
from mbs.schedule import Schedule
from mbs.globals import State, EventType

from mbs.target import CloudBlockStorageSnapshotReference


from robustify.robustify import robustify
from mbs.errors import (
    raise_if_not_retriable, raise_exception, BackupSweepError)

from mbs.utils import document_pretty_string

from mbs.notification.handler import NotificationPriority, NotificationType


###############################################################################
# LOGGER
###############################################################################

logger = mbs_logging.simple_file_logger("BackupSweeper", "sweeper.log")

###############################################################################
# BackupSweeper
###############################################################################

DEFAULT_SWEEP_SCHEDULE = Schedule(frequency_in_seconds=12 * 60 * 60)
DEFAULT_DELETE_DELAY_IN_SECONDS = 5 * 24 * 60 * 60  # 5 days

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
        self._worker_count = 0
        self._sweep_workers = None
        self._sweep_queue = multiprocessing.JoinableQueue()

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
            get_mbs().notifications.send_notification(subject, message, notification_type=NotificationType.EVENT,
                                                      priority=NotificationPriority.CRITICAL)

    ###########################################################################
    def _delete_backups_targets_due(self):

        logger.info("BackupSweeper: Starting a sweep cycle...")

        # clear stats
        self._cycle_total_processed = 0
        self._cycle_total_errored = 0
        self._cycle_total_deleted = 0
        # compute # of workers based on cpu count
        self._worker_count = multiprocessing.cpu_count() * 2 + 1
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

        backups_iter = get_mbs().backup_collection.find_iter(query=q, no_cursor_timeout=True)

        backups_iterated = 0
        # process all plan backups
        for backup in backups_iter:
            if self.stop_requested:
                break

            self._sweep_queue.put(backup)
            backups_iterated += 1
            # PERFORMANCE OPTIMIZATION
            # process 10 * worker at max
            # This is needed because making backup objects (from within the backups_iter) takes up a lot of CPU/Memory
            # This is needed to give it a breath
            if backups_iterated % (self._worker_count * 10) == 0:
                self._wait_for_queue_to_be_empty()

        self._finish_cycle()


        logger.info("BackupSweeper: Finished sweep cycle. "
                    "Total Deleted=%s, Total Errored=%s, "
                    "Total Processed=%s" %
                    (self._cycle_total_deleted,
                     self._cycle_total_errored,
                     self._cycle_total_processed))

    ###########################################################################
    def _start_workers(self):
        for i in xrange(self._worker_count):
            sweep_worker = SweepWorker(self, self._sweep_queue)
            self._sweep_workers.append(sweep_worker)
            sweep_worker.start()

    ###########################################################################
    def _finish_cycle(self):
        self._wait_for_queue_to_be_empty()
        self._stop_and_wait_for_all_workers_to_finish()

    ###########################################################################
    def _wait_for_queue_to_be_empty(self):
        self._sweep_queue.join()

    ###########################################################################
    def _stop_and_wait_for_all_workers_to_finish(self):
        # request stop
        for i in xrange(self._worker_count):
            # put a None for each worker to stop
            self._sweep_queue.put(None)

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
                self.robustified_delete_backup(backup)
                return True
            else:
                logger.info("NOOP. Running in test mode. Not deleting "
                            "targets for backup '%s'" % backup.id)
        except Exception, e:
            msg = "Error while attempting to delete backup '%s': %s" % (backup.id, e)
            logger.exception(msg)
            get_mbs().notifications.send_notification("Backup Delete Error",
                                                      msg, notification_type=NotificationType.EVENT,
                                                      priority=NotificationPriority.CRITICAL)


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
    # EXPIRE/DELETE BACKUP HELPERS
    ###############################################################################
    @robustify(max_attempts=3, retry_interval=5,
               do_on_exception=raise_if_not_retriable,
               do_on_failure=raise_exception)
    def robustified_delete_backup(self, backup):
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
            self.do_delete_target_ref(backup, backup.target, target_ref)

        # delete log file
        if backup.log_target_reference:
            logger.info("Deleting log target reference for backup '%s'." %
                        backup.id)
            self.do_delete_target_ref(backup, backup.target, backup.log_target_reference)

        if backup.secondary_target_references:
            logger.info("Deleting secondary target references for backup '%s'." %
                        backup.id)
            sec_targets = backup.secondary_targets
            sec_target_refs = backup.secondary_target_references
            for (sec_target, sec_tgt_ref) in zip(sec_targets, sec_target_refs):
                logger.info("Deleting secondary target reference %s for backup "
                            "'%s'." % (sec_tgt_ref, backup.id))
                self.do_delete_target_ref(backup, sec_target, sec_tgt_ref)

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
    def do_delete_target_ref(self, backup, target, target_ref):

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
        except Exception as e:
            if self.is_whitelisted_target_delete_error(backup, target, target_ref, e):
                msg = ("Caught a whitelisted error while attempting to delete backup %s."
                       " Marking backup as deleted. Error: %s" % (backup.id, e))
                logger.warn(msg)
                persistence.update_backup(backup,
                                          event_name="WHITELIST_DELETE_ERROR",
                                          message=msg,
                                          event_type=EventType.WARNING)
                return False
            else:
                # raise error
                raise

    ###############################################################################
    def is_whitelisted_target_delete_error(self, backup, target, target_ref, e):
        return False

###############################################################################


class SweepWorker(multiprocessing.Process):
    """
        A Thread that periodically expire backups that are due for expiration
    """
    ###########################################################################
    def __init__(self, backup_sweeper, sweep_queue):
        multiprocessing.Process.__init__(self)
        self._stats = multiprocessing.Manager().dict()
        self._backup_sweeper = backup_sweeper
        self._sweep_queue = sweep_queue

        self.total_processed = 0
        self.total_deleted = 0
        self.total_errored = 0

    ###########################################################################
    @property
    def total_processed(self):
        return self._stats["total_processed"]

    @total_processed.setter
    def total_processed(self, val):
        self._stats["total_processed"] = val

    ###########################################################################
    @property
    def total_deleted(self):
        return self._stats["total_deleted"]

    @total_deleted.setter
    def total_deleted(self, val):
        self._stats["total_deleted"] = val

    ###########################################################################
    @property
    def total_errored(self):
        return self._stats["total_errored"]

    @total_errored.setter
    def total_errored(self, val):
        self._stats["total_errored"] = val

    ###########################################################################
    def run(self):
        while True:

            backup = self._sweep_queue.get()
            if backup is None: # None in Queue means STOP!!!
                logger.info("%s Exiting..." % self.name)
                self._sweep_queue.task_done()
                # breaking
                break

            logger.info("%s Processing backup %s" % (self.name, backup.id))
            self.total_processed += 1
            try:
                deleted = self._backup_sweeper.delete_backup_targets(backup)
                if deleted:
                    self.total_deleted += 1
            except Exception, ex:
                self.total_errored += 1
                msg = ("%s: Error while attempting to "
                       "delete backup targets for backup '%s'" % (self.name, backup.id))
                logger.exception(msg)
            finally:
                self._sweep_queue.task_done()




