__author__ = 'abdul'

from task import *
from bson.dbref import DBRef

###############################################################################
# Restore
###############################################################################
class Restore(MBSTask):
    def __init__(self):
        # init fields
        MBSTask.__init__(self)
        self._source_backup = None
        self._destination = None
        self._destination_stats = None

    ###########################################################################
    def execute(self):
        """
            Override
        """
        return self.strategy.run_restore(self)

    ###########################################################################
    def cleanup(self):
        """
            Override
        """
        return self.strategy.cleanup_restore(self)

    ###########################################################################
    @property
    def source_backup(self):
        return self._source_backup

    @source_backup.setter
    def source_backup(self, source_backup):
        self._source_backup = source_backup

    ###########################################################################
    @property
    def destination(self):
        return self._destination

    @destination.setter
    def destination(self, destination):
        self._destination = destination

    ###########################################################################
    @property
    def destination_stats(self):
        return self._destination_stats

    @destination_stats.setter
    def destination_stats(self, destination_stats):
        self._destination_stats = destination_stats

    ###########################################################################
    def to_document(self, display_only=False):
        doc = MBSTask.to_document(self, display_only=display_only)
        doc.update({
            "_type": "Restore",
            "sourceBackup": DBRef("backups", self.source_backup.id),
            "destination": self.destination.to_document(display_only=
                                                         display_only),
            "destinationStats": self.destination_stats
        })

        return doc

    ###########################################################################