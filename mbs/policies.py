__author__ = 'abdul'

import traceback
import mbs_logging
from mbs import get_mbs
from base import MBSObject
from date_utils import date_now, date_minus_seconds

###############################################################################
# Contains Backup Retention Policies
###############################################################################

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
# RetentionPolicy
###############################################################################
class RetentionPolicy(MBSObject):

    ###########################################################################
    def __init__(self):
        MBSObject.__init__(self)

    ###########################################################################

    def apply_policy(self, plan):
        """
            Applies the retention policy by deleting target references for
            expired succeeded backups (e.g. deleting backup files for expired
            backups)
        """
        policy_name = self.__class__.__name__
        for backup in self.get_expired_backups(plan):
            try:
                backup_collection = get_mbs().backup_collection
                # Block other threads (through DB) from doing same operation
                q = {
                    "_id": backup.id,
                    "targetReference": {"$exists": True},
                    "$or": [
                            {"targetReference.expired": {"$exists": False}},
                            {"targetReference.expired": False}
                    ]
                }

                u = {
                    "$set": {"targetReference.expired": True}
                }
                backup = backup_collection.find_and_modify(q,u)
                if backup:
                    logger.info("%s: Expiring backup %s and deleting backup "
                                "file" %
                                (policy_name, backup.id))
                    backup.target_reference.expired = True
                    backup.target.delete_file(backup.target_reference)
                    backup_collection.save_document(backup.to_document())
                    logger.info("%s: Backup %s archived successfully!" %
                                (policy_name, backup.id))
            except Exception, e:
                logger.error("%s: Error while archiving backup %s. "
                             "Trace: %s" %
                             (policy_name, backup.id, traceback.format_exc()))

    ###########################################################################
    def get_expired_backups(self, plan):
        """
            Returns a list of backups that should expired and should be
            removed. Should be overridden by sub classes
        """
        return []


###############################################################################
# RetainLastNPolicy
###############################################################################
class RetainLastNPolicy(RetentionPolicy):
    """
        Retains the last 'n' backups
    """
    ###########################################################################
    def __init__(self):
        RetentionPolicy.__init__(self)
        self._retain_count = 0

    ###########################################################################
    @property
    def retain_count(self):
        return self._retain_count

    @retain_count.setter
    def retain_count(self, retain_count):
        self._retain_count = retain_count

    ###########################################################################
    def get_expired_backups(self, plan):
        q = {
            "plan._id": plan.id,
            "targetReference": {"$exists": True},
            # Filter out backups with targetReference.expired is set to true
            "$or": [
                    {"targetReference.expired": {"$exists": False}},
                    {"targetReference.expired": False}
            ]
        }
        s = [("startDate", -1)]

        backups = get_mbs().backup_collection.find(q, sort=s)

        if len(backups) <= self.retain_count:
            return []
        else:
            return backups[self.retain_count:]

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "RetainLastNPolicy",
            "retainCount": self.retain_count
        }


###############################################################################
# RetainTimePolicy
###############################################################################
class RetainMaxTimePolicy(RetentionPolicy):
    """
        Retains T time worth of data. i.e. Backup date is within now() - T
    """
    ###########################################################################
    def __init__(self):
        RetentionPolicy.__init__(self)
        self._max_time = 0

    ###########################################################################
    @property
    def max_time(self):
        return self._max_time

    @max_time.setter
    def max_time(self, max_time):
        self._max_time = max_time

    ###########################################################################
    def get_expired_backups(self, plan):

        earliest_date_to_keep = date_minus_seconds(date_now(), self.max_time)
        q = {
            "plan._id": plan.id,
            "targetReference": {"$exists": True},
            # Filter out backups with targetReference.expired is set to true
            "$or": [
                    {"targetReference.expired": {"$exists": False}},
                    {"targetReference.expired": False}
            ],
            "startDate": {
                "$lt": earliest_date_to_keep
            }
        }

        backups = get_mbs().backup_collection.find(q)
        return backups

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "RetainMaxTimePolicy",
            "maxTime": self.max_time
        }