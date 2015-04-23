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
        self._run_plan_generators()

    def _run_plan_generators(self):
        logger.info("Running ALL plan generators")
        for generator in self._backup_system.plan_generators:
            self._run_generator(generator)

    ####################################################################################################################
    def _run_generator(self, generator):
        logger.info("Running plan generator '%s' " % generator.name)
        # remove expired plans
        for plan in generator.get_plans_to_remove():
            self._backup_system.remove_plan(plan.id)

        # save new plans
        for plan in generator.get_plans_to_save():
            self._backup_system.save_plan(plan)