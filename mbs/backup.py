__author__ = 'abdul'

from date_utils import date_now
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
# event types
EVENT_TYPE_INFO = "INFO"
EVENT_TYPE_WARNING = "WARNING"
EVENT_TYPE_ERROR = "ERROR"

# Priority constants
PRIORITY_HIGH = 0
PRIORITY_MEDIUM = 5
PRIORITY_LOW = 10

###############################################################################
# Backup
###############################################################################
class Backup(MBSObject):
    def __init__(self):
        # init fields
        self._id = None
        self._created_date = None
        self._name = None
        self._state = None
        self._strategy = None
        self._source = None
        self._source_stats = None
        self._target = None
        self._target_reference = None
        self._plan = None
        self._plan_occurrence = None
        self._engine_guid = None
        self._logs = []
        self._backup_rate_in_mbps = None
        self._start_date = None
        self._end_date = None
        self._tags = None
        self._try_count = 0
        self._reschedulable = None
        self._workspace = None
        self._priority = PRIORITY_LOW

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = str(id)

    ###########################################################################
    @property
    def created_date(self):
        return self._created_date

    @created_date.setter
    def created_date(self, created_date):
        self._created_date = created_date

    ###########################################################################
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    ###########################################################################
    def change_state(self, state, message=None):
        """
        Updates the state and logs a state change event
        """
        if state != self.state:
            self.state = state
            return self.log_event(name=EVENT_STATE_CHANGE, message=message)

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
    def engine_guid(self):
        return self._engine_guid

    @engine_guid.setter
    def engine_guid(self, engine_guid):
        self._engine_guid = engine_guid

    ###########################################################################
    @property
    def logs(self):
        return self._logs

    @logs.setter
    def logs(self, logs):
        self._logs = logs


    ###########################################################################
    @property
    def backup_rate_in_mbps(self):
        return self._backup_rate_in_mbps

    @backup_rate_in_mbps.setter
    def backup_rate_in_mbps(self, backup_rate):
        self._backup_rate_in_mbps = backup_rate

    ###########################################################################
    @property
    def start_date(self):
        return self._start_date

    @start_date.setter
    def start_date(self, start_date):
        self._start_date = start_date

    ###########################################################################
    @property
    def end_date(self):
        return self._end_date

    @end_date.setter
    def end_date(self, end_date):
        self._end_date = end_date

    ###########################################################################
    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        self._tags = tags

    ###########################################################################
    @property
    def try_count(self):
        return self._try_count

    @try_count.setter
    def try_count(self, try_count):
        self._try_count = try_count

    ###########################################################################
    @property
    def reschedulable(self):
        return self._reschedulable

    @reschedulable.setter
    def reschedulable(self, val):
        self._reschedulable = val

    ###########################################################################
    @property
    def workspace(self):
        return self._workspace

    @workspace.setter
    def workspace(self, val):
        self._workspace = val

    ###########################################################################
    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, val):
        self._priority = val

    ###########################################################################
    def log_event(self, event_type=EVENT_TYPE_INFO, name=None, message=None,
                        details=None):
        logs = self.logs

        log_entry = BackupLogEntry()
        log_entry.event_type = event_type
        log_entry.name = name
        log_entry.date = date_now()
        log_entry.state = self.state
        log_entry.message = message
        log_entry.details = details

        logs.append(log_entry)
        self.logs = logs
        return log_entry

    ###########################################################################
    def has_errors(self):
        return len(self.get_errors()) > 0

    ###########################################################################
    def has_warnings(self):
        return len(self.get_warnings()) > 0

    ###########################################################################
    def get_errors(self):
        return self._get_logs_by_event_type(EVENT_TYPE_ERROR)

    ###########################################################################
    def get_warnings(self):
        return self._get_logs_by_event_type(EVENT_TYPE_WARNING)

    ###########################################################################
    def _get_logs_by_event_type(self, event_type):
        return filter(lambda entry: entry.event_type == event_type, self.logs)

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
    def to_document(self, display_only=False):
        doc = {
            "_type": "Backup",
            "createdDate": self.created_date,
            "state": self.state,
            "strategy": self.strategy.to_document(display_only=display_only),
            "source": self.source.to_document(display_only=display_only),
            "target": self.target.to_document(display_only=display_only),
            "planOccurrence": self.plan_occurrence,
            "engineGuid": self.engine_guid,
            "logs": self.export_logs(),
            "workspace": self.workspace
        }

        if self.id:
            doc["_id"] = self.id

        if self.name:
            doc["name"] = self.name

        if self.plan:
            doc["plan"] = self.plan.to_document(display_only=display_only)

        if self.target_reference:
            doc["targetReference"] = self.target_reference.to_document(
                                                     display_only=display_only)

        if self.source_stats:
            doc["sourceStats"] = self.source_stats

        if self.backup_rate_in_mbps:
            doc["backupRateInMBPS"] = self.backup_rate_in_mbps

        if self.start_date:
            doc["startDate"] = self.start_date

        if self.end_date:
            doc["endDate"] = self.end_date

        if self.tags:
            doc["tags"] = self.tags

        if self.try_count:
            doc["tryCount"] = self.try_count

        if self.reschedulable is not None:
            doc["reschedulable"] = self.reschedulable

        if self.priority is not None:
            doc["priority"] = self.priority

        return doc

    ###########################################################################
    def export_logs(self, event_type=None):
        result = []
        logs = self.logs
        if event_type:
            logs = self._get_logs_by_event_type(event_type=event_type)

        for log_entry in logs:
            result.append(log_entry.to_document())

        return result


###############################################################################
# BackupLogEntry
###############################################################################
class BackupLogEntry(MBSObject):

    ###########################################################################
    def __init__(self):
        self._event_type = None
        self._name = None
        self._date = None
        self._state = None
        self._message = None
        self._details = None

    ###########################################################################
    @property
    def event_type(self):
        return self._event_type

    @event_type.setter
    def event_type(self, value):
        self._event_type = value

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
    @property
    def details(self):
        return self._details

    @details.setter
    def details(self, value):
        self._details = value

    ###########################################################################
    def to_document(self, display_only=False):
        doc= {
            "_type": "BackupLogEntry",
            "eventType": self.event_type,
            "date": self.date,
            "state": self.state
        }
        if self.name:
            doc["name"] = self.name

        if self.message:
            doc["message"] = self.message

        if self.details:
            doc["details"] = self.details

        return doc

###############################################################################
# Helpers
###############################################################################
def state_change_log_entry(state, message=None):

    log_entry = BackupLogEntry()
    log_entry.event_type = EVENT_TYPE_INFO
    log_entry.name = EVENT_STATE_CHANGE
    log_entry.date = date_now()
    log_entry.state = state
    log_entry.message = message


    return log_entry