__author__ = 'abdul'

from backup import EVENT_TYPE_INFO
from mbs import get_mbs
from utils import listify
# Contains helper functions for persisting mbs documents

###############################################################################

def update_backup(backup, properties=None, event_name=None,
                  event_type=EVENT_TYPE_INFO, message=None, details=None):
    backup_doc = backup.to_document()
    q = {
        "_id": backup.id
    }

    u = {}
    # construct $set operator
    if properties:
        properties = listify(properties)
        u["$set"] = {}
        for prop in properties:
            u["$set"][prop] = backup_doc.get(prop)


    # construct the $push

    if event_name or message:
        log_entry = backup.log_event(name=event_name, event_type=event_type,
                                     message=message, details=details)
        u["$push"] = {"logs": log_entry.to_document()}

    get_mbs().backup_collection.update(spec=q, document=u)