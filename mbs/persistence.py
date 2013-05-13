__author__ = 'abdul'

from backup import EVENT_TYPE_INFO
from mbs import get_mbs
from utils import listify

from target import CloudBlockStorageSnapshotReference
import  mbs_logging
###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger


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



###############################################################################
def expire_backup(backup, expired_date):
    """
        expires the backup
    """

    backup_collection = get_mbs().backup_collection
    # Block other threads (through DB) from doing same operation
    q = {
        "_id": backup.id,
        "targetReference": {"$exists": True},
        "$or": [
                {"targetReference.expiredDate": {"$exists": False}},
                {"targetReference.expiredDate": None}
        ]
    }
    u = {
        "$set": {"targetReference.expiredDate": expired_date}
    }
    backup = backup_collection.find_and_modify(query=q, update=u)
    if backup:
        logger.info("Expiring backup '%s'" %  backup.id)

        target_ref = backup.target_reference

        # if the target reference is a cloud storage one then make the cloud
        # storage object take care of it
        if isinstance(target_ref, CloudBlockStorageSnapshotReference):
            logger.info("Deleting backup '%s' snapshot " % backup.id)
            target_ref.cloud_block_storage.delete_snapshot(target_ref)
        else:
            logger.info("Deleting backup '%s file" % backup.id)
            backup.target.delete_file(target_ref)

        backup.target_reference.expired_date = expired_date
        # no need to persist the expiredDate since
        update_backup(backup, event_name="EXPIRING", message="Expiring")

        logger.info("Backup %s archived successfully!" % backup.id)