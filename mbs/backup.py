__author__ = 'abdul'

from task import *

###############################################################################
# Backup
###############################################################################
class Backup(MBSTask):
    def __init__(self):
        # init fields
        MBSTask.__init__(self)
        self._name = None
        self._source = None
        self._selected_sources = None
        self._source_stats = None
        self._target = None
        self._secondary_targets = None
        self._target_reference = None
        self._secondary_target_references = None
        self._plan = None
        self._plan_occurrence = None
        self._backup_rate_in_mbps = None
        self._expired_date = None
        self._dont_expire = False
        self._deleted_date = None
        self._data_stats = {}
        self._cluster_stats = None

    ###########################################################################
    def execute(self):
        """
            Override
        """
        return self.strategy.run_backup(self)

    ###########################################################################
    def cleanup(self):
        """
            Override
        """
        return self.strategy.cleanup_backup(self)

    ###########################################################################
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    ###########################################################################
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        self._source = source

    ###########################################################################
    @property
    def selected_sources(self):
        return self._selected_sources

    @selected_sources.setter
    def selected_sources(self, val):
        self._selected_sources = val

    ###########################################################################
    @property
    def source_stats(self):
        return self._source_stats

    @source_stats.setter
    def source_stats(self, source_stats):
        self._source_stats = source_stats

    ###########################################################################
    @property
    def cluster_stats(self):
        return self._cluster_stats

    @cluster_stats.setter
    def cluster_stats(self, val):
        self._cluster_stats = val

    ###########################################################################
    @property
    def target(self):
        return self._target


    @target.setter
    def target(self, target):
        self._target = target

    ###########################################################################
    @property
    def secondary_targets(self):
        return self._secondary_targets

    @secondary_targets.setter
    def secondary_targets(self, val):
        self._secondary_targets = val

    ###########################################################################
    @property
    def target_reference(self):
        return self._target_reference


    @target_reference.setter
    def target_reference(self, target_reference):
        self._target_reference = target_reference

    ###########################################################################
    @property
    def secondary_target_references(self):
        return self._secondary_target_references

    @secondary_target_references.setter
    def secondary_target_references(self, vals):
        self._secondary_target_references = vals

    ###########################################################################
    def get_any_active_secondary_target(self):
        """

        :return: A tuple of any target/target-ref that is active
        (i.e. not deleted)
        """
        if self.secondary_target_references:
            for target, target_ref in zip(self.secondary_targets,
                                          self.secondary_target_references):
                if not target_ref.deleted:
                    return target, target_ref

        return None, None

    ###########################################################################
    @property
    def plan(self):
        return self._plan

    @plan.setter
    def plan(self, plan):
        self._plan = plan

    ###########################################################################
    @property
    def plan_occurrence(self):
        return self._plan_occurrence

    @plan_occurrence.setter
    def plan_occurrence(self, plan_occurrence):
        self._plan_occurrence = plan_occurrence


    ###########################################################################
    @property
    def backup_rate_in_mbps(self):
        return self._backup_rate_in_mbps

    @backup_rate_in_mbps.setter
    def backup_rate_in_mbps(self, backup_rate):
        self._backup_rate_in_mbps = backup_rate


    ###########################################################################
    @property
    def expired(self):
        return self.expired_date is not None

    ###########################################################################
    @property
    def deleted(self):
        return self.deleted_date is not None

    ###########################################################################
    @property
    def expired_date(self):
        return self._expired_date

    @expired_date.setter
    def expired_date(self, expired_date):
        self._expired_date = expired_date

    ###########################################################################
    @property
    def deleted_date(self):
        return self._deleted_date

    @deleted_date.setter
    def deleted_date(self, val):
        self._deleted_date = val

    ###########################################################################
    @property
    def dont_expire(self):
        return self._dont_expire

    @dont_expire.setter
    def dont_expire(self, val):
        self._dont_expire = val

    ###########################################################################
    @property
    def data_stats(self):
        return self._data_stats

    @data_stats.setter
    def data_stats(self, val):
        self._data_stats = val

    ###########################################################################
    def to_document(self, display_only=False):

        doc = MBSTask.to_document(self, display_only=display_only)
        doc.update({
            "_type": "Backup",
            "source": self.source and self.source.to_document(display_only=display_only),
            "target": self.target and self.target.to_document(display_only=display_only),
            "planOccurrence": self.plan_occurrence,
            "expiredDate": self.expired_date,
            "dontExpire": self.dont_expire,
            "deletedDate": self.deleted_date,
            "clusterStats": self.cluster_stats
        })

        if self.name:
            doc["name"] = self.name

        if self.plan:
            doc["plan"] = self.plan.to_document(display_only=display_only)

        if self.target_reference:
            doc["targetReference"] = self.target_reference.to_document(
                display_only=display_only)

        if self.secondary_targets:
            doc["secondaryTargets"] = \
                map(lambda t: t.to_document(display_only=display_only),
                    self.secondary_targets)

        if self.secondary_target_references:
            doc["secondaryTargetReferences"] = \
                map(lambda tr: tr.to_document(display_only=display_only),
                    self.secondary_target_references)

        if self._selected_sources:
            doc["selectedSources"] = \
                map(lambda s: s.to_document(display_only=display_only), self.selected_sources)

        if self.source_stats:
            doc["sourceStats"] = self.source_stats

        if self.backup_rate_in_mbps:
            doc["backupRateInMbps"] = self.backup_rate_in_mbps

        if self.data_stats:
            doc["dataStats"] = self.data_stats

        return doc