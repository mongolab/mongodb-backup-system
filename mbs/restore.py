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
        self._source_database_name = None
        self._destination = None
        self._destination_stats = None
        self._dump_collection_counts = None
        self._restore_collection_counts = None
        self._valid = None

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
    def source_database_name(self):
        return self._source_database_name

    @source_database_name.setter
    def source_database_name(self, source_database_name):
        self._source_database_name = source_database_name

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
    @property
    def dump_collection_counts(self):
        return self._dump_collection_counts

    @dump_collection_counts.setter
    def dump_collection_counts(self, val):
        self._dump_collection_counts = val

    ###########################################################################
    @property
    def restore_collection_counts(self):
        return self._restore_collection_counts

    @restore_collection_counts.setter
    def restore_collection_counts(self, val):
        self._restore_collection_counts = val

    ###########################################################################
    @property
    def valid(self):
        return self._valid

    @valid.setter
    def valid(self, valid):
        self._valid = valid

    ###########################################################################
    def to_document(self, display_only=False):
        doc = MBSTask.to_document(self, display_only=display_only)
        doc.update({
            "_type": "Restore",
            "sourceBackup": DBRef("backups", self.source_backup.id),
            "sourceDatabaseName": self.source_database_name,
            "destination": self.destination.to_document(display_only=display_only),
            "dumpCollectionCounts": self.dump_collection_counts,
            "restoreCollectionCounts": self.restore_collection_counts,
            "valid": self.valid
        })

        return doc
