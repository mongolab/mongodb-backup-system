__author__ = 'abdul'

from globals import EventType
from mbs import get_mbs
from mongo_utils import objectiditify
import  mbs_logging
###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
# Contains helper functions for persisting mbs documents
###############################################################################
def get_backup(backup_id):
    return get_mbs().backup_collection.get_by_id(backup_id)

###############################################################################
def get_backup_plan(plan_id):
    return get_mbs().plan_collection.get_by_id(objectiditify(plan_id))

###############################################################################
def get_restore(restore_id):
    return get_mbs().restore_collection.get_by_id(restore_id)

###############################################################################
def update_backup(backup, properties=None, event_name=None,
                  event_type=EventType.INFO, message=None, details=None):
    bc = get_mbs().backup_collection
    bc.update_task(backup, properties=properties, event_name=event_name,
                   event_type=event_type, message=message, details=details,
                   w=1)

###############################################################################
def update_restore(restore, properties=None, event_name=None,
                   event_type=EventType.INFO, message=None, details=None):
    rc = get_mbs().restore_collection
    rc.update_task(restore, properties=properties, event_name=event_name,
                   event_type=event_type, message=message, details=details,
                   w=1)
