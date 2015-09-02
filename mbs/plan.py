__author__ = 'abdul'

from base import MBSObject

from globals import Priority

###############################################################################
# BackupPlan
###############################################################################
class BackupPlan(MBSObject):

    def __init__(self):
        MBSObject.__init__(self)
        self._id = None
        self._created_date = None
        self._deleted_date = None
        self._description = None
        self._source = None
        self._target = None
        self._secondary_targets = None
        self._schedule = None
        self._next_occurrence = None
        self._strategy = None
        self._retention_policy = None
        self._generator = None
        self._tags = None
        self._priority = Priority.LOW

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    ###########################################################################
    @property
    def created_date(self):
        return self._created_date

    @created_date.setter
    def created_date(self, created_date):
        self._created_date = created_date

    ###########################################################################
    @property
    def deleted_date(self):
        return self._deleted_date

    @deleted_date.setter
    def deleted_date(self, deleted_date):
        self._deleted_date = deleted_date

    ###########################################################################
    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description = description

    ###########################################################################
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        self._source = source

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
    def schedule(self):
        return self._schedule

    @schedule.setter
    def schedule(self, schedule):
        self._schedule = schedule

    ###########################################################################
    @property
    def next_occurrence(self):
        return self._next_occurrence

    @next_occurrence.setter
    def next_occurrence(self, next_occurrence):
        self._next_occurrence = next_occurrence

    ###########################################################################
    @property
    def strategy(self):
        return self._strategy

    @strategy.setter
    def strategy(self, strategy):
        self._strategy = strategy

    ###########################################################################
    @property
    def retention_policy(self):
        return self._retention_policy

    @retention_policy.setter
    def retention_policy(self, retention_policy):
        self._retention_policy = retention_policy

    ###########################################################################
    @property
    def generator(self):
        return self._generator

    @generator.setter
    def generator(self, generator):
        self._generator = generator

    ###########################################################################
    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        self._tags = tags

    ###########################################################################
    def get_tag(self, name):
        if self.tags:
            return self.tags.get(name)

    ###########################################################################
    def _export_tags(self):
        if self.tags:
            exported_tags = {}
            for name,value in self.tags.items():
                if isinstance(value, MBSObject):
                    exported_tags[name]= value.to_document()
                else:
                    exported_tags[name] = value

            return exported_tags

    ###########################################################################
    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, val):
        self._priority = val

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "_type": "Plan",
            "createdDate": self.created_date,
            "deletedDate": self.deleted_date,
            "description": self.description,
            "source": self.source.to_document(display_only=display_only),
            "target": self.target.to_document(display_only=display_only),
            "schedule": self.schedule.to_document(display_only=display_only),
            "nextOccurrence": self.next_occurrence,
            "strategy": self.strategy.to_document(display_only=display_only),
            "priority": self.priority,
            "generator": self.generator
        }

        if self.id:
            doc["_id"] = self.id

        if self.retention_policy:
            doc["retentionPolicy"] = self.retention_policy.to_document(
                                                    display_only=display_only)

        if self.tags:
            doc["tags"] = self._export_tags()

        if self.secondary_targets:
            doc["secondaryTargets"] = \
                map(lambda t: t.to_document(display_only=display_only),
                    self.secondary_targets)

        return doc

    ###########################################################################
    def is_valid(self):
        errors = self.validate()
        if errors:
            return False
        else:
            return True

    ###########################################################################
    def validate(self):
        """
         Returns an array containing validation messages (if any). Empty if no
         validation errors
        """
        errors = []

        #  schedule
        if not self.schedule:
            errors.append("Missing plan 'schedule'")
        else:
            schedule_errors = self.schedule.validate()
            if schedule_errors:
                errors.append("Invalid 'schedule'")
                errors.extend(schedule_errors)

        # validate source
        if not self.source:
            errors.append("Missing plan 'source'")
        else:
            source_errors = self.source.validate()
            if source_errors:
                errors.append("Invalid 'source'")
                errors.extend(source_errors)

        # validate target
        if not self.target:
            errors.append("Missing plan 'target'")
        else:
            target_errors = self.target.validate()
            if target_errors:
                errors.append("Invalid 'target'")
                errors.extend(target_errors)

        # validate strategy
        if not self.strategy:
            errors.append("Missing plan 'strategy'")

        return errors

