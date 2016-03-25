__author__ = 'abdul'

import logging
import time

from base import MBSObject
from threading import Thread
from date_utils import date_now
from werkzeug.contrib.cache import SimpleCache
########################################################################################################################
# LOGGER
########################################################################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())



########################################################################################################################
# EventQueue
########################################################################################################################
class EventQueue(object):

    ####################################################################################################################
    def create_event(self, event):
        raise Exception("Need to be implemented")


########################################################################################################################
# Event
########################################################################################################################
class Event(MBSObject):

    ####################################################################################################################
    def __init__(self):
        super(Event, self).__init__()
        self._created_date = None

    ####################################################################################################################
    @property
    def created_date(self):
        return self._created_date

    @created_date.setter
    def created_date(self, value):
        self._created_date = value

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(Event, self).to_document(display_only=display_only)

        doc.update({
            "createdDate": self.created_date
        })

        return doc


########################################################################################################################
# BackupEvent
########################################################################################################################
class BackupEvent(Event):
    """
    Base class for all backup events
    """
    ####################################################################################################################
    def __init__(self, backup=None):
        super(BackupEvent, self).__init__()
        self._backup = backup

    ####################################################################################################################
    @property
    def backup(self):
        return self._backup

    @backup.setter
    def backup(self, backup):
        self._backup = backup

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(Event, self).to_document(display_only=display_only)

        doc.update({
            "_type": "BackupEvent",
            "createdDate": self.created_date,
            "backup": self.backup.to_document(display_only=display_only)
        })

        return doc

########################################################################################################################
# BackupFinishedEvent
########################################################################################################################
class BackupFinishedEvent(BackupEvent):
    """
    Triggered when a backup finishes (goes to FAILED or SUCCEEDED state)
    """
    ####################################################################################################################
    def __init__(self, backup=None, state=None):
        super(BackupFinishedEvent, self).__init__(backup=backup)
        self._state = state

    ####################################################################################################################
    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state


    ####################################################################################################################
    #TODO XXX Remove this when all clients no longer use the "context" field and use "backup"/"state" properties
    # as direct properties
    @property
    def context(self):
        return None

    @context.setter
    def context(self, value):
        if value:
            if "backup" in value:
                self.backup = value["backup"]
            if "state" in value:
                self.state = value["state"]

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(BackupFinishedEvent, self).to_document(display_only=display_only)
        doc.update({
            "_type": "BackupFinishedEvent",
            "state": self.state
        })

        return doc



########################################################################################################################
# RestoreEvent
########################################################################################################################
class RestoreEvent(Event):
    """
    Base class for all restore events
    """
    ####################################################################################################################
    def __init__(self, restore=None):
        super(RestoreEvent, self).__init__()
        self._restore = restore

    ####################################################################################################################
    @property
    def restore(self):
        return self._restore

    @restore.setter
    def restore(self, restore):
        self._restore = restore

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(Event, self).to_document(display_only=display_only)

        doc.update({
            "_type": "RestoreEvent",
            "createdDate": self.created_date,
            "restore": self.restore.to_document(display_only=display_only)
        })

        return doc

########################################################################################################################
# RestoreFinishedEvent
########################################################################################################################
class RestoreFinishedEvent(RestoreEvent):
    """
    Triggered when a restore finishes (goes to FAILED or SUCCEEDED state)
    """
    ####################################################################################################################
    def __init__(self, restore=None, state=None):
        super(RestoreFinishedEvent, self).__init__(restore=restore)
        self._state = state

    ####################################################################################################################
    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(RestoreFinishedEvent, self).to_document(display_only=display_only)
        doc.update({
            "_type": "RestoreFinishedEvent",
            "state": self.state
        })

        return doc