__author__ = 'greg'

from .date_utils import date_now
from .plan import BackupPlan

###############################################################################
# DeletedBackupPlan
###############################################################################
class DeletedBackupPlan(BackupPlan):

    def __init__(self):
        BackupPlan.__init__(self)
        self._deleted_date = None

    ###########################################################################
    @property
    def deleted_date(self):
        return self._deleted_date

    @deleted_date.setter
    def deleted_date(self, deleted_date):
        self._deleted_date = deleted_date

    ###########################################################################
    def to_document(self, display_only=False):
        doc = BackupPlan.to_document(self, display_only)
        doc.update({
            "_type": "DeletedPlan",
            "deletedDate": self.deleted_date
        })

        return doc

    ###########################################################################
    @staticmethod
    def from_plan(plan):
        """Generate a DeletedBackupPlan from a BackupPlan

        """
        doc = plan.to_document()
        doc.update({
            "_type": "DeletedPlan",
            "deletedDate": date_now()
        })
        from .mbs import get_mbs
        return get_mbs().deleted_plan_collection.make_obj(doc)

