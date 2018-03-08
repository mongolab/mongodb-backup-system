__author__ = 'abdul'

import operator


from mbs.base import MBSObject
from mbs.date_utils import date_now, date_minus_seconds, date_plus_seconds


###############################################################################
# RetentionPolicy
###############################################################################
class RetentionPolicy(MBSObject):

    ###########################################################################
    def __init__(self):
        MBSObject.__init__(self)

    ###########################################################################
    def filter_backups_due_for_expiration(self, backups):
        """
            Returns a list of backups that should expired and should be
            removed. Should be overridden by sub classes
        """
        return []

    ###########################################################################
    def get_plan_occurrences_to_retain_as_of(self, plan, dt):
        pass

    ###########################################################################
    def get_occurrence_expected_expire_date(self, plan, occurrence):
        pass

###############################################################################
# RetainLastNPolicy
###############################################################################
class RetainLastNPolicy(RetentionPolicy):
    """
        Retains the last 'n' backups
    """
    ###########################################################################
    def __init__(self, retain_count=5):
        RetentionPolicy.__init__(self)
        self._retain_count = retain_count

    ###########################################################################
    @property
    def retain_count(self):
        return self._retain_count

    @retain_count.setter
    def retain_count(self, retain_count):
        self._retain_count = retain_count

    ###########################################################################
    def filter_backups_due_for_expiration(self, backups):

        backups.sort(key=operator.attrgetter('created_date'), reverse=True)

        if len(backups) <= self.retain_count:
            return []
        else:
            return backups[self.retain_count:]

    ###########################################################################
    def get_plan_occurrences_to_retain_as_of(self, plan, dt):
        return plan.schedule.last_n_occurrences(self.retain_count, dt=dt)

    ###########################################################################
    def get_occurrence_expected_expire_date(self, plan, occurrence):
        # get n occurrences to keep as of this occurrence and return the
        # last one ;)
        dt = date_plus_seconds(occurrence, 1)
        ocs = plan.schedule.next_n_occurrences(self.retain_count,
                                               dt=occurrence)
        return ocs[-1]

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "RetainLastNPolicy",
            "retainCount": self.retain_count
        }


###############################################################################
# RetainTimePolicy
###############################################################################
class RetainMaxTimePolicy(RetentionPolicy):
    """
        Retains T time worth of data. i.e. Backup date is within now() - T
    """
    ###########################################################################
    def __init__(self, max_time=0):
        RetentionPolicy.__init__(self)
        self._max_time = max_time

    ###########################################################################
    @property
    def max_time(self):
        return self._max_time

    @max_time.setter
    def max_time(self, max_time):
        self._max_time = max_time

    ###########################################################################
    def filter_backups_due_for_expiration(self, backups):

        earliest_date_to_keep = date_minus_seconds(date_now(), self.max_time)

        return filter(lambda backup:
                      backup.created_date < earliest_date_to_keep,
                      backups)

    ###########################################################################
    def get_plan_occurrences_to_retain_as_of(self, plan, dt):
        end_date = dt
        start_date = date_minus_seconds(end_date, self.max_time)
        return plan.schedule.natural_occurrences_between(start_date, end_date)

    ###########################################################################
    def get_occurrence_expected_expire_date(self, plan, occurrence):
        return date_plus_seconds(occurrence, self.max_time)

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "RetainMaxTimePolicy",
            "maxTime": self.max_time
        }
