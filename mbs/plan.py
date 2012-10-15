__author__ = 'abdul'

from datetime import timedelta
from utils import (seconds_to_date, date_to_seconds, date_plus_seconds,
                   date_now, document_pretty_string, epoch_date,
                   is_date_value)

from errors import ConfigurationError

###############################################################################
# BackupPlan
###############################################################################
class BackupPlan(object):
    def __init__(self):
        self._id = None
        self._active = True
        self._source = None
        self._target = None
        self._schedule = None
        self._next_occurrence = None
        self._errors = None

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = str(id)

    ###########################################################################
    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, active):
        self._active = active

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
    def errors(self):
        return self._errors

    @errors.setter
    def errors(self, errors):
        self._errors = errors

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
            "source": self.source.to_document(),
            "target": self.target.to_document(),
            "schedule": self.schedule.to_document(),
            "nextOccurrence": self.next_occurrence,
            "active": self.active
        }

        if self.id:
            doc["_id"] = self.id

        if self.errors:
            doc["errors"] = self.errors

        return doc

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())

    ###########################################################################
    def validate(self):
        errors = []
        #  id
        if not self.id:
            errors.append("Missing plan '_id'")

        #  schedule
        if not self.schedule:
            errors.append("Missing plan 'schedule'")

        #  frequency
        if not self.schedule.frequency:
            errors.append("Plan schedule is missing 'frequency'")

        # offset
        if self.schedule.offset and not is_date_value(self.schedule.offset):
            errors.append("Invalid plan schedule offset '%s'. "
                                     "offset has to be a date" %
                                     self.schedule.offset)

        # validate source
        if not self.source:
            errors.append("Missing plan 'source'")
        else:
            errors.extend(self.source.validate())

        return errors

        # TODO validate target

###############################################################################
# Schedule
###############################################################################
class Schedule(object):
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
    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())
