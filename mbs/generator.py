__author__ = 'abdul'

import logging
from schedule import Schedule
from schedule_runner import ScheduleRunner

from mbs import get_mbs
from notification.handler import NotificationPriority

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

########################################################################################################################
DEFAULT_GEN_SCHEDULE = Schedule(frequency_in_seconds=(30 * 60))

###############################################################################
# PlanGenerator
# An abstraction of something that generate/removes backup plans. This is used
# By the backup system
###############################################################################
class PlanGenerator(ScheduleRunner):

    ###########################################################################
    def __init__(self):
        schedule = DEFAULT_GEN_SCHEDULE
        ScheduleRunner.__init__(self, schedule=schedule)
        self._backup_system = None

    ###########################################################################
    @property
    def name(self):
        return ""

    ###########################################################################
    @property
    def backup_system(self):
        return self._backup_system

    @backup_system.setter
    def backup_system(self, value):
        self._backup_system = value

    ###########################################################################
    def get_plans_to_save(self):
        return []

    ###########################################################################
    def get_plans_to_remove(self):
        return []

    ####################################################################################################################
    def tick(self):
        self.run_generator()

    ####################################################################################################################
    def run_generator(self, dry_run=False):

        try:
            if dry_run:
                logger.info("----- DRY RUN ------")

            logger.info("Running plan generator '%s' " % self.name)
            # remove expired plans
            for plan in self.get_plans_to_remove():
                if not dry_run:
                    self._backup_system.remove_plan(plan.id)
                else:
                    logger.info("DRY RUN: remove plan '%s' " % plan.id)

            # save new plans
            for plan in self.get_plans_to_save():
                try:
                    if not dry_run:
                        self._backup_system.save_plan(plan)
                    else:
                        logger.info("DRY RUN: save plan: %s" % plan)
                except Exception, ex:
                    logger.exception("Error while saving plan %s" % plan)

                    get_mbs().notifications.send_event_notification("Error in saving plan for generator %s" %
                                                                    self.name,
                                                                    str(ex), priority=NotificationPriority.CRITICAL)
        except Exception, ex:
            logger.exception("Error in running plan generator %s" % self.name)

            get_mbs().notifications.send_event_notification("Error in running plan generator %s" % self.name,
                                                            str(ex), priority=NotificationPriority.CRITICAL)
