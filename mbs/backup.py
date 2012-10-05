__author__ = 'abdul'

from utils import document_pretty_string, date_now

###############################################################################
# CONSTANTS
###############################################################################
STATE_SCHEDULED = "SCHEDULED"
STATE_IN_PROGRESS = "IN PROGRESS"
STATE_FAILED = "FAILED"
STATE_CANCELED = "CANCELED"
STATE_SUCCEEDED = "SUCCEEDED"

###############################################################################
# Backup
###############################################################################
class Backup(object):
    def __init__(self):
        # init fields
        self._id = None
        self._state = None
        self._source = None
        self._target = None
        self._plan = None
        self._plan_occurrence = None
        self._timestamp = None
        self._engine_id = None
        self._logs = []

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = str(id)

    ###########################################################################
    def change_state(self, state):
        """
        Updates the state and logs a state change event
        """
        if state != self.state:
            self.state = state
            self.log_event("STATE_CHANGE")

    ###########################################################################
    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

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
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        self._timestamp = timestamp

    ###########################################################################
    @property
    def engine_id(self):
        return self._engine_id

    @timestamp.setter
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
    def log_event(self, message):
        logs = self.logs

        log_entry = BackupLogEntry()
        log_entry.date = date_now()
        log_entry.state = self.state
        log_entry.message = message

        logs.append(log_entry)
        self.logs = logs

    ###########################################################################
    def to_document(self):
        doc = {
            "_type": "Backup",
            "state": self.state,
            "source": self.source.to_document(),
            "target": self.target.to_document(),
            "plan": self.plan.to_document(),
            "planOccurrence": self.timestamp,
            "timestamp": self.timestamp,
            "engineId": self.engine_id,
            "logs": self.export_logs()
        }

        if self.id:
            doc["_id"] = self.id

        return doc

    ###########################################################################
    def export_logs(self):
        result = []
        for log_entry in self.logs:
            result.append(log_entry.to_document())

        return result

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())


###############################################################################
# BackupLogEntry
###############################################################################
class BackupLogEntry(object):
    def __init__(self):
        self._date = None
        self._state = None
        self._message = None

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
            "date": self.date,
            "state": self.state,
            "message": self.message
        }

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())