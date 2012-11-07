__author__ = 'abdul'

from base import MBSObject
from datetime import timedelta
from date_utils import (seconds_to_date, date_to_seconds, date_plus_seconds,
                        date_now, is_date_value, epoch_date)

###############################################################################
# CONSTANTS
###############################################################################

STRATEGY_DUMP = "DUMP"
STRATEGY_EBS_SNAPSHOT = "EBS_SNAPSHOT"
STRATEGY_DB_FILES = "DB_FILES"

ALL_STRATEGIES = [STRATEGY_DUMP, STRATEGY_EBS_SNAPSHOT, STRATEGY_DB_FILES]

###############################################################################
# BackupPlan
###############################################################################
class BackupPlan(MBSObject):
    def __init__(self):
        self._id = None
        self._description = None
        self._source = None
        self._target = None
        self._schedule = None
        self._next_occurrence = None
        self._strategy = None
        self._retention_policy = None
        self._generator = None
        self._tags = None

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

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
    def next_natural_occurrence(self):

        last_natural_occurrence = self.last_natural_occurrence()
        frequency = self.schedule.frequency
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
                                schedule.frequency))

    ###########################################################################
    def natural_occurrences_as_of(self, date):
        next_date = date + timedelta(days=1)
        return self.natural_occurrences_between(date, next_date)

    ###########################################################################
    def natural_occurrences_between(self, start_date, end_date):
        occurrences = []
        last_occurrence = self.last_natural_occurrence_as_of(start_date)

        delta = timedelta(seconds=self.schedule.frequency)

        while last_occurrence < end_date:
            if last_occurrence >= start_date:
                occurrences.append(last_occurrence)

            last_occurrence = last_occurrence + delta

        return occurrences

    ###########################################################################
    def to_document(self):
        doc = {
            "_type": "Plan",
            "description": self.description,
            "source": self.source.to_document(),
            "target": self.target.to_document(),
            "schedule": self.schedule.to_document(),
            "nextOccurrence": self.next_occurrence,
            "strategy": self.strategy
        }

        if self.id:
            doc["_id"] = self.id

        if self.retention_policy:
            doc["retentionPolicy"] = self.retention_policy.to_document()

        if self.generator:
            doc["generator"] = self.generator

        if self.tags:
            doc["tags"] = self.tags

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
            if not self.schedule.frequency:
                errors.append("Plan schedule is missing 'frequency'")
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
        elif self.strategy not in ALL_STRATEGIES:
            errors.append("Unknown plan strategy '%s'" % self.strategy)

        return errors

###############################################################################
# Schedule
###############################################################################
class Schedule(MBSObject):
    def __init__(self):
        self._frequency = None
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
    def frequency(self):
        return self._frequency

    @frequency.setter
    def frequency(self, frequency):
        self._frequency = frequency

    ###########################################################################
    def to_document(self):
        return {
            "_type": "Schedule",
            "frequency": self.frequency,
            "offset": self.offset
        }
