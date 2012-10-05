__author__ = 'abdul'

from datetime import timedelta
from utils import (today_date, seconds_now, seconds_to_date, date_to_seconds,
                   date_plus_seconds, yesterday_date, document_pretty_string)


###############################################################################
# BackupPlan
###############################################################################
class BackupPlan(object):
    def __init__(self):
        self._id = None
        self._source = None
        self._target = None
        self._schedule = None
        self._next_occurrence = None

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = str(id)
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
    def next_natural_occurrence(self):

        last_natural_occurrence = self.last_natural_occurrence()
        frequency = self.schedule.frequency
        return date_plus_seconds(last_natural_occurrence, frequency)

    ###########################################################################
    def last_natural_occurrence(self):
        schedule = self.schedule
        now_seconds = seconds_now()
        offset_seconds = date_to_seconds(self.schedule.offset)
        return seconds_to_date(now_seconds - ((now_seconds - offset_seconds) %
                                              schedule.frequency))

    ###########################################################################
    def natural_occurrences_yesterday(self):
        occurrences = []
        last_occurrence = self.last_natural_occurrence()
        yesterday = yesterday_date()
        today = today_date()
        delta = timedelta(seconds=self.schedule.frequency)

        while last_occurrence >= yesterday:
            if last_occurrence < today:
                occurrences.append(last_occurrence)
            last_occurrence = last_occurrence - delta

        return occurrences

    ###########################################################################
    def to_document(self):
        return {
            "_id": self.id,
            "_type": "Plan",
            "source": self.source.to_document(),
            "target": self.target.to_document(),
            "schedule": self.schedule.to_document(),
            "nextOccurrence": self.next_occurrence
        }

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())

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
