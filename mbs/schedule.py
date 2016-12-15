import abc

from datetime import datetime, timedelta

from croniter import croniter

from base import MBSObject
from date_utils import (seconds_to_date, date_to_seconds, date_plus_seconds,
                        date_now, is_date_value, epoch_date,
                        timedelta_total_seconds)


###############################################################################
# AbstractSchedule
###############################################################################
class AbstractSchedule(object):
    __metaclass__ = abc.ABCMeta

    ###########################################################################
    @abc.abstractmethod
    def validate(self):
        """ Validate the schedule.

        Returns a list of validation errors.

        """
        pass

    ###########################################################################
    def _max_acceptable_lag_for_period(self, td):
        """ Get the max acceptable lagtime for the timedelta between backups.


        args:
            td      - the timedelta between backups
        """
        return int(timedelta_total_seconds(td) / 2)

    ###########################################################################
    @abc.abstractmethod
    def max_acceptable_lag(self, dt=None):
        """ Return the maximum acceptable lag for a backup scheduled at dt.

        Return value is in seconds.

        args:
            dt      - the datetime for which we are looking for the maximum
                      acceptable lag of secondary.

        """
        pass

    ###########################################################################
    @abc.abstractmethod
    def next_natural_occurrence(self, dt=None):
        """ Get the next occurrence of a scheduled action relative to dt.

        NOTE: If dt is a natural occurrence, this returns the next occurrence.

        args:
            dt      - the datetime for which the next relative occurrence of
                      a scheduled action should be returned (if None, use the
                      current time)

        """
        pass

    ###########################################################################
    @abc.abstractmethod
    def last_natural_occurrence(self, dt=None):
        """ Get the previous occurrence of a scheduled action relative to dt.

        NOTE: If dt is a natural occurrence, it is returned.

        args:
            dt      - the datetime for which the previous relative occurrence
                      of a scheduled action should be returned (if None, use
                      the current time)

        """
        pass

    ###########################################################################
    @abc.abstractmethod
    def natural_occurrences_between(self, start_dt, end_dt=None):
        """ Get the scheduled occurrences between two dates

        NOTE: The occurrences returned are not inclusive of end_dt.

        args:
            start_dt    - the starting datetime for the period over which all
                          occurrences of a scheduled action should be returned
            end_dt      - the ending datetime of the period (if None, end_dt
                          will default to the current time)

        """
        if end_dt is None:
            end_dt = date_now()
        if end_dt <= start_dt:
            raise Exception('end_dt must be greater than start_dt')

    ###########################################################################
    def natural_occurrences_as_of(self, date):
        next_date = date + timedelta(days=1)
        return self.natural_occurrences_between(date, next_date)

    ###########################################################################
    def last_n_occurrences(self, n, dt=None):
        end_date = dt or date_now()
        occurrences = []
        for i in range(0, n):
            occurrence = self.last_natural_occurrence(dt=end_date)
            occurrences.append(occurrence)
            end_date = occurrence - self.min_time_delta()

        return occurrences

    ###########################################################################
    def next_n_occurrences(self, n, dt=None):
        start_date = dt or date_now()
        occurrences = []
        for i in range(0, n):
            occurrence = self.next_natural_occurrence(dt=start_date)
            occurrences.append(occurrence)
            start_date = occurrence + self.min_time_delta()

        return occurrences

    ###########################################################################
    def min_time_delta(self):
        return timedelta(seconds=1)

###############################################################################
# Schedule
###############################################################################
class Schedule(AbstractSchedule, MBSObject):
    def __init__(self, frequency_in_seconds=None, offset=None):
        self._frequency_in_seconds = frequency_in_seconds
        self._offset = offset or epoch_date()

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
    def validate(self):
        errors = []
        #  frequency
        if not self.frequency_in_seconds:
            errors.append("Plan schedule is missing 'frequencyInSeconds'")
        # offset
        if not self.offset:
            errors.append("Plan schedule is missing 'offset'")
        elif not is_date_value(self.offset):
            errors.append("Invalid plan schedule offset '%s'. "
                          "offset has to be a date" % (self.offset))
        return errors

    ###########################################################################
    def max_acceptable_lag(self, dt=None):
        """ NOTE: dt is not necessary since we have a constant period

        """
        return self._max_acceptable_lag_for_period(
                    timedelta(seconds=self.frequency_in_seconds))

    ###########################################################################
    def next_natural_occurrence(self, dt=None):
        last_natural_occurrence = self.last_natural_occurrence(dt)
        frequency = self.frequency_in_seconds
        return date_plus_seconds(last_natural_occurrence, frequency)

    ###########################################################################
    def last_natural_occurrence(self, dt=None):
        dt = date_now() if dt is None else dt
        date_seconds = date_to_seconds(dt)
        offset = self.offset if self.offset else epoch_date()
        offset_seconds = date_to_seconds(offset)

        return seconds_to_date(date_seconds -
                               ((date_seconds - offset_seconds) %
                                self.frequency_in_seconds))

    ###########################################################################
    def natural_occurrences_between(self, start_dt, end_dt=None):
        super(Schedule, self).natural_occurrences_between(start_dt, end_dt)

        end_dt = date_now() if end_dt is None else end_dt
        occurrences = []
        last_occurrence = self.last_natural_occurrence(start_dt)

        delta = timedelta(seconds=self.frequency_in_seconds)

        while last_occurrence < end_dt:
            if last_occurrence >= start_dt:
                occurrences.append(last_occurrence)

            last_occurrence = last_occurrence + delta

        return occurrences

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "Schedule",
            "frequencyInSeconds": self.frequency_in_seconds,
            "offset": self.offset
        }


###############################################################################
# CronSchedule
###############################################################################
class CronSchedule(AbstractSchedule, MBSObject):
    def __init__(self):
        self._expression = None

    ###########################################################################
    @property
    def expression(self):
        return self._expression

    @expression.setter
    def expression(self, expression):
        self._expression = expression

    ###########################################################################
    def _is_occurrence(self, dt):
        occurrence = croniter(self._expression, dt).get_prev(datetime)
        if dt == croniter(self._expression, occurrence).get_next(datetime):
            return True
        return False

    ###########################################################################
    def validate(self):
        errors = []
        if not self.expression:
            errors.append("Plan schedule is missing expression")
        else:
            try:
                croniter(self.expression)
            except (ValueError, KeyError), e:
                errors.append("Plan schedule has an invalid expression (%s): "
                              "%s" % (self.expression, str(e)))
        return errors

    ###########################################################################
    def max_acceptable_lag(self, dt=None):
        dt = date_now() if dt is None else dt
        if self._is_occurrence(dt):
            # we are concerned with the period leading up to dt
            return self._max_acceptable_lag_for_period(
                        dt - self.last_natural_occurrence(
                                    dt - timedelta(minutes=1)))
        # otherwise we are concerned with the period leading up to the next
        # occurrence
        return self._max_acceptable_lag_for_period(
                    self.next_natural_occurrence(dt) -
                    self.last_natural_occurrence(dt))

    ###########################################################################
    def next_natural_occurrence(self, dt=None):
        dt = date_now() if dt is None else dt
        return croniter(self._expression, dt).get_next(datetime)

    ###########################################################################
    def last_natural_occurrence(self, dt=None):
        """ NOTE: cron is a minute level resolution. round up in the case of
                  seconds/microseconds

        """
        dt = date_now() if dt is None else dt
        if dt.second > 0 or dt.microsecond > 0:
            dt = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        if self._is_occurrence(dt):
            return dt
        return croniter(self._expression, dt).get_prev(datetime)

    ###########################################################################
    def natural_occurrences_between(self, start_dt, end_dt=None):
        super(CronSchedule, self).natural_occurrences_between(start_dt, end_dt)
        end_dt = date_now() if end_dt is None else end_dt
        occurrences = []
        if self._is_occurrence(start_dt):
            occurrences.append(start_dt)
        iter_ = croniter(self._expression, start_dt)
        occurrences.append(iter_.get_next(datetime))
        while occurrences[-1] < end_dt:
            occurrences.append(iter_.get_next(datetime))
        return occurrences[:-1]

    ###########################################################################
    def min_time_delta(self):
        return timedelta(minutes=1)

        ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "CronSchedule",
            "expression": self.expression
        }


########################################################################################################################
# CompositeSchedule
########################################################################################################################
class CompositeSchedule(AbstractSchedule, MBSObject):

    ####################################################################################################################
    def __init__(self, schedules=None):
        self._schedules = schedules

    ####################################################################################################################
    @property
    def schedules(self):
        return self._schedules

    ####################################################################################################################
    @schedules.setter
    def schedules(self, schedules):
        self._schedules = schedules

    ####################################################################################################################
    def max_acceptable_lag(self, dt=None):
        """
            :return max acceptable lag by all schedules
        """
        lags = map(lambda s: s.max_acceptable_lag(), self.schedules)
        lags.sort()
        return lags[-1]

    ####################################################################################################################
    def next_natural_occurrence(self, dt=None):
        """
            :return min next natural occurrence across all schedules
        """
        ocs = map(lambda s: s.next_natural_occurrence(), self.schedules)
        ocs.sort()
        return ocs[0]

    ###########################################################################
    def last_natural_occurrence(self, dt=None):
        """
            :return max last next occurrence across all schedules
        """
        ocs = map(lambda s: s.last_natural_occurrence(), self.schedules)
        ocs.sort()
        return ocs[-1]

    ###########################################################################
    def natural_occurrences_between(self, start_dt, end_dt=None):
        """
            :returns all occurrences across all schedules
        """
        all_ocs = []
        for s in self.schedules:
            all_ocs.extend(s.natural_occurrences_between(start_dt, end_dt=end_dt))

        # eliminate duplicates
        all_ocs = list(set(all_ocs))
        all_ocs.sort()
        return all_ocs

    ###########################################################################
    def validate(self):
        errors = []
        if not self.schedules:
            errors.append("CompositeSchedule missing schedules")
        return errors

