__author__ = 'abdul'

from utils import date_now
from base import MBSObject
###############################################################################
# CONSTANTS
###############################################################################
STATE_SCHEDULED = "SCHEDULED"
STATE_IN_PROGRESS = "IN PROGRESS"
STATE_FAILED = "FAILED"
STATE_CANCELED = "CANCELED"
STATE_SUCCEEDED = "SUCCEEDED"

EVENT_STATE_CHANGE = "STATE_CHANGE"

###############################################################################
# Backup
###############################################################################
class Backup(MBSObject):
    def __init__(self):
        # init fields
        self._id = None
        self._state = None
        self._strategy = None
        self._source = None
        self._source_stats = None
        self._target = None
        self._target_reference = None
        self._plan = None
        self._plan_occurrence = None
        self._engine_id = None
        self._logs = []
        self._backup_rate = None

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = str(id)

    ###########################################################################
    def change_state(self, state, message=None):
        """
        Updates the state and logs a state change event
        """
        if state != self.state:
            self.state = state
            self.log_event(EVENT_STATE_CHANGE, message=message)

    ###########################################################################
    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

    ###########################################################################
    @property
    def strategy(self):
        return self._strategy

    @strategy.setter
    def strategy(self, strategy):
        self._strategy = strategy

    ###########################################################################
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        self._source = source

    ###########################################################################
    @property
    def source_stats(self):
        return self._source_stats

    @source_stats.setter
    def source_stats(self, source_stats):
        self._source_stats = source_stats

    ###########################################################################
    @property
    def target(self):
        return self._target


    @target.setter
    def target(self, target):
        self._target = target

    ###########################################################################
    @property
    def target_reference(self):
        return self._target_reference


    @target_reference.setter
    def target_reference(self, target_reference):
        self._target_reference = target_reference

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
    def engine_id(self):
        return self._engine_id

    @engine_id.setter
    def engine_id(self, engine_id):
        self._engine_id = engine_id

    ###########################################################################
    @property
    def logs(self):
        return self._logs

    @logs.setter
    def logs(self, logs):
        self._logs = logs


    ###########################################################################
    @property
    def backup_rate(self):
        return self._backup_rate

    @backup_rate.setter
    def backup_rate(self, backup_rate):
        self._backup_rate = backup_rate

    ###########################################################################
    @property
    def start_date(self):
        """
            Returns the date the backup was started
            (i.e. date of 'IN PROGRESS' state)
        """
        return self._get_state_set_date(STATE_IN_PROGRESS)

    ###########################################################################
    def log_event(self, name, message=None):
        logs = self.logs

        log_entry = BackupLogEntry()
        log_entry.name = name
        log_entry.date = date_now()
        log_entry.state = self.state
        log_entry.message = message

        logs.append(log_entry)
        self.logs = logs

    ###########################################################################
    def is_event_logged(self, event_name):
        """
            Accepts an event name or a list of event names
            Returns true if logs contain an event with the specified
            event_name

        """
        if not isinstance(event_name, list):
            event_name = [event_name]

        logs = filter(lambda entry: entry.name in event_name, self.logs)
        return len(logs) > 0

    ###########################################################################
    def _get_state_set_date(self, state):
        """
           Returns the date of when the backup was set to the specified state.
           None if state was never set
        """

        state_logs = filter(lambda entry: entry.state == state, self.logs)
        if state_logs:
            return state_logs[0].date

    ###########################################################################
    def to_document(self):
        doc = {
            "_type": "Backup",
            "state": self.state,
            "strategy": self.strategy,
            "source": self.source.to_document(),
            "target": self.target.to_document(),
            "plan": self.plan.to_document(),
            "planOccurrence": self.plan_occurrence,
            "engineId": self.engine_id,
            "logs": self.export_logs()
        }

        if self.id:
            doc["_id"] = self.id

        if self.target_reference:
            doc["targetReference"] = self.target_reference.to_document()

        if self.source_stats:
            doc["sourceStats"] = self.source_stats

        if self.backup_rate:
            doc["backupRate"] = self.backup_rate

        return doc

    ###########################################################################
    def export_logs(self):
        result = []
        for log_entry in self.logs:
            result.append(log_entry.to_document())

        return result


###############################################################################
# BackupLogEntry
###############################################################################
class BackupLogEntry(MBSObject):

    ###########################################################################
    def __init__(self):
        self._name = None
        self._date = None
        self._state = None
        self._message = None

    ###########################################################################
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    ###########################################################################
    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, value):
        self._date = value

    ###########################################################################
    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    ###########################################################################
    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, value):
        self._message = value

    ###########################################################################
    def to_document(self):
        return {
            "_type": "BackupLogEntry",
            "name": self.name,
            "date": self.date,
            "state": self.state,
            "message": self.message
        }