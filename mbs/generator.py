__author__ = 'abdul'

import mbs_logging

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
# PlanGenerator
# An abstraction of something that generate/removes backup plans. This is used
# By the plan manager
###############################################################################
class PlanGenerator(object):

    ###########################################################################
    def __init__(self, plan_collection):
        self._plan_collection = plan_collection

    ###########################################################################
    def run(self):
        # add new plans
        for plan in self.get_new_plans():
            logger.info("PlanGenerator: Generating new plan:\n%s " % plan)
            self._plan_collection.save_document(plan.to_document())

        # remove expired plans
        for plan in self.get_expired_plans():
            logger.info("PlanGenerator: Removing expired plan:\n%s " % plan)
            self._plan_collection.remove_by_id(plan.id)

    ###########################################################################
    def get_new_plans(self):
        return []

    ###########################################################################
    def get_expired_plans(self):
        return []
