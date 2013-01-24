__author__ = 'abdul'

from base import MBSObject
from datetime import timedelta
from date_utils import (seconds_to_date, date_to_seconds, date_plus_seconds,
                        date_now, is_date_value, epoch_date)

from backup import PRIORITY_LOW

###############################################################################
# BackupPlan
###############################################################################
class BackupPlan(MBSObject):
    def __init__(self):
        self._id = None
        self._created_date = None
        self._description = None
        self._source = None
        self._target = None
        self._schedule = None
        self._next_occurrence = None
        self._strategy = None
        self._retention_policy = None
        self._generator = None
        self._tags = None
        self._backup_naming_scheme = None
        self._priority = PRIORITY_LOW

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
    @property
    def backup_naming_scheme(self):
        return self._backup_naming_scheme

    @backup_naming_scheme.setter
    def backup_naming_scheme(self, naming_scheme):
        self._backup_naming_scheme = naming_scheme


    ###########################################################################
    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, val):
        self._priority = val

    ###########################################################################
    def next_natural_occurrence(self):

        last_natural_occurrence = self.last_natural_occurrence()
        frequency = self.schedule.frequency_in_seconds
        return date_plus_seconds(last_natural_occurrence, frequency)

    ###########################################################################
    def last_natural_occurrence(self):
        return self.last_natural_occurrence_as_of(date_now())

    ###########################################################################
    def last_natural_occurrence_as_of(self, date):
        schedule = self.schedule
        date_seconds = date_to_seconds(date)
        offset = schedule.offset if schedule.offset else epoch_date()
        offset_seconds = date_to_seconds(offset)

        return seconds_to_date(date_seconds -
                               ((date_seconds - offset_seconds) %
                                schedule.frequency_in_seconds))

    ###########################################################################
    def natural_occurrences_as_of(self, date):
        next_date = date + timedelta(days=1)
        return self.natural_occurrences_between(date, next_date)

    ###########################################################################
    def natural_occurrences_between(self, start_date, end_date):
        occurrences = []
        last_occurrence = self.last_natural_occurrence_as_of(start_date)

        delta = timedelta(seconds=self.schedule.frequency_in_seconds)

        while last_occurrence < end_date:
            if last_occurrence >= start_date:
                occurrences.append(last_occurrence)

            last_occurrence = last_occurrence + delta

        return occurrences

    ###########################################################################
    def get_backup_name(self, backup):
        naming_scheme = self.backup_naming_scheme
        if not naming_scheme:
            naming_scheme = DefaultBackupNamingScheme()
        elif type(naming_scheme) in [unicode, str]:
            name_template = naming_scheme
            naming_scheme = TemplateBackupNamingScheme(template=name_template)

        return naming_scheme.get_backup_name(backup)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = {
            "_type": "Plan",
            "createdDate": self.created_date,
            "description": self.description,
            "source": self.source.to_document(display_only=display_only),
            "target": self.target.to_document(display_only=display_only),
            "schedule": self.schedule.to_document(display_only=display_only),
            "nextOccurrence": self.next_occurrence,
            "strategy": self.strategy.to_document(display_only=display_only)
        }

        if self.id:
            doc["_id"] = self.id

        if self.retention_policy:
            doc["retentionPolicy"] = self.retention_policy.to_document(
                                                    display_only=display_only)

        if self.generator:
            doc["generator"] = self.generator

        if self.tags:
            doc["tags"] = self.tags

        if self.backup_naming_scheme:
            doc["backupNamingScheme"] = self.backup_naming_scheme

        if self.priority:
            doc["priority"] = self.priority

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
            #  frequency
            if not self.schedule.frequency_in_seconds:
                errors.append("Plan schedule is missing 'frequencyInSeconds'")
            # offset
            if (self.schedule.offset and
                not is_date_value(self.schedule.offset)):
                errors.append("Invalid plan schedule offset '%s'. "
                                         "offset has to be a date" %
                                         self.schedule.offset)

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

###############################################################################
# Schedule
###############################################################################
class Schedule(MBSObject):
    def __init__(self):
        self._frequency_in_seconds = None
        self._offset = None

    ###########################################################################
    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, offset):
        self._offset = offset

    ###########################################################################
    @property
    def frequency_in_seconds(self):
        return self._frequency_in_seconds

    @frequency_in_seconds.setter
    def frequency_in_seconds(self, frequency):
        self._frequency_in_seconds = frequency

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "Schedule",
            "frequencyInSeconds": self.frequency_in_seconds,
            "offset": self.offset
        }


###############################################################################
# BackupNamingScheme
###############################################################################
class BackupNamingScheme(MBSObject):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def get_backup_name(self, backup):
        pass

###############################################################################
# DefaultBackupNamingScheme
###############################################################################
class DefaultBackupNamingScheme(BackupNamingScheme):

    ###########################################################################
    def __init__(self):
        BackupNamingScheme.__init__(self)

    ###########################################################################
    def get_backup_name(self, backup):
        return "%s" % backup.id

###############################################################################
# TemplateBackupNamingScheme
###############################################################################
class TemplateBackupNamingScheme(BackupNamingScheme):

    ###########################################################################
    def __init__(self, template=None):
        BackupNamingScheme.__init__(self)
        self._template = template

    ###########################################################################
    def get_backup_name(self, backup):
        return self.template.format(backup=backup)

    ###########################################################################
    @property
    def template(self):
        return self._template

    @template.setter
    def template(self, template):
        self._template = template

    ###########################################################################