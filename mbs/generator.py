__author__ = 'abdul'

import mbs_logging
from schedule import Schedule
from schedule_runner import ScheduleRunner

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
# PlanGenerator
# An abstraction of something that generate/removes backup plans. This is used
# By the backup system
###############################################################################
class PlanGenerator(object):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    @property
    def name(self):
        return ""
    ###########################################################################
    def get_plans_to_save(self):
        return []

    ###########################################################################
    def get_plans_to_remove(self):
        return []

########################################################################################################################
DEFAULT_GEN_SCHEDULE = Schedule(frequency_in_seconds=(30 * 60))

class PlanGenerationRunner(ScheduleRunner):
    ####################################################################################################################
    def __init__(self, backup_system, schedule=None):
        schedule = schedule or DEFAULT_GEN_SCHEDULE
        ScheduleRunner.__init__(self, schedule=schedule)
        self._backup_system = backup_system


    ####################################################################################################################
    def tick(self):
        self.run_plan_generators()

    ####################################################################################################################
    def run_plan_generators(self, dry_run=False):
        logger.info("Running ALL plan generators")
        if dry_run:
            logger.info("----- DRY RUN ------")
        for generator in self._backup_system.plan_generators:
            self._run_generator(generator, dry_run=dry_run)

    ####################################################################################################################
    def _run_generator(self, generator, dry_run=False):
        logger.info("Running plan generator '%s' " % generator.name)
        # remove expired plans
        for plan in generator.get_plans_to_remove():
            if not dry_run:
                self._backup_system.remove_plan(plan.id)
            else:
                logger.info("DRY RUN: remove plan '%s' " % plan.id)

        # save new plans
        for plan in generator.get_plans_to_save():
            if not dry_run:
                self._backup_system.save_plan(plan)
            else:
                logger.info("DRY RUN: save plan: %s" % plan)