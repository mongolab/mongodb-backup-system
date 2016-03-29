__author__ = 'abdul'

from threading import Thread
import time
import logging

from schedule import Schedule
from date_utils import date_now
from utils import wait_for
from mbs import get_mbs
import traceback
###############################################################################
# ScheduleRunner
###############################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# default period is 10 seconds

DEFAULT_SCHEDULE = Schedule(frequency_in_seconds=10)
DEFAULT_SLEEP = 1

class ScheduleRunner(Thread):
    ###########################################################################
    def __init__(self, schedule=DEFAULT_SCHEDULE, sleep_time=DEFAULT_SLEEP):
        Thread.__init__(self)
        self.daemon = True
        self._schedule = schedule
        self._stop_requested = False
        self._sleep_time = sleep_time
        self._stopped = False

    ###########################################################################
    @property
    def schedule(self):
        return self._schedule

    @schedule.setter
    def schedule(self, val):
        self._schedule = val

    ###########################################################################
    @property
    def stop_requested(self):
        return self._stop_requested

    ###########################################################################
    def run(self):

        try:
            while not self.stop_requested:
                next_occurrence = self._schedule.next_natural_occurrence()
                while date_now() < next_occurrence and not self._stop_requested:
                    time.sleep(self._sleep_time)

                # break if stop requested
                if self._stop_requested:
                    break

                try:
                    self.tick()
                except Exception, ex:
                    logger.exception("ScheduleRunner.tick() error")
                    get_mbs().notifications.send_error_notification("ScheduleRunner.tick() error",
                                                                    traceback.format_exc())
        except Exception, ex:
            logger.exception("ScheduleRunner.run() error")
            get_mbs().notifications.send_error_notification("ScheduleRunner.run() error",
                                                            traceback.format_exc())
        self._stopped = True

    ###########################################################################
    def tick(self):
        """
            To be overridden
        """
        print "TICK"

    ###########################################################################
    def stop(self, blocking=False):
        """
        """
        name = self.__class__.__name__
        logger.info("%s: Stop requested" % name)
        self._stop_requested = True

        if blocking:
            def stopped():
                return self._stopped

            logger.info("%s: Waiting to stop" % name)
            wait_for(stopped, timeout=60)

            if stopped():
                logger.info("%s: stopped successfully." % name)
            else:
                raise Exception("%s did not stop in 60 seconds")







