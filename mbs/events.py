__author__ = 'abdul'

import logging
import time

from base import MBSObject
from threading import Thread
from date_utils import date_now
from werkzeug.contrib.cache import SimpleCache
###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())



###############################################################################
# EventQueue
###############################################################################
class EventQueue(object):

    ###########################################################################
    def create_event(self, event):
        raise Exception("Need to be implemented")



###############################################################################
# EventListener
###############################################################################
class EventListener(MBSObject):

    ###########################################################################
    def __init__(self):
        super(EventListener, self).__init__()
        self._name = None
        self._event_types = None

    ###########################################################################
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    ###########################################################################
    @property
    def event_types(self):
        return self._event_types

    @event_types.setter
    def event_types(self, value):
        self._event_types = value

    ###########################################################################
    def handle_event(self, event):
        pass

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(EventListener, self).to_document(display_only=display_only)
        doc.update({
            "name": self.name,
            "eventTypes:": self.event_types
        })
        return doc

###############################################################################
# Event
###############################################################################
class Event(MBSObject):

    ###########################################################################
    def __init__(self):
        super(Event, self).__init__()
        self._event_type = None
        self._created_date = None
        self._context = {}
        self._subscribed_event_listeners = []

    ###########################################################################
    @property
    def event_type(self):
        return self._event_type

    @event_type.setter
    def event_type(self, value):
        self._event_type = value

    ###########################################################################
    @property
    def created_date(self):
        return self._created_date

    @created_date.setter
    def created_date(self, value):
        self._created_date = value

    ###########################################################################
    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, value):
        self._context = value

    ###########################################################################
    @property
    def subscribed_event_listeners(self):
        return self._subscribed_event_listeners

    @subscribed_event_listeners.setter
    def subscribed_event_listeners(self, value):
        self._subscribed_event_listeners = value

    ###########################################################################
    def _export_context(self, display_only=False):
        exported_context = {}
        if self.context:
            for name,value in self.context.items():
                if isinstance(value, MBSObject):
                    exported_context[name] = value.to_document(display_only=display_only)
                else:
                    exported_context[name] = value

        return exported_context

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(Event, self).to_document(display_only=display_only)

        doc.update({
            "_type": "Event",
            "eventType": self.event_type,
            "createdDate": self.created_date,
            "context": self._export_context(display_only=display_only),
            "subscribedEventListeners": self.subscribed_event_listeners
        })

        return doc

###############################################################################
# Backup Event Types

class BackupEventTypes(object):
    BACKUP_FINISHED = "BackupFinished"

###############################################################################
# BackupEvent
###############################################################################
class BackupEvent(Event):
    """
    Base class for all backup events
    """
    ###########################################################################
    def __init__(self, backup=None):
        super(BackupEvent, self).__init__()
        self.backup = backup

    ###########################################################################
    @property
    def backup(self):
        return self.context.get("backup")

    @backup.setter
    def backup(self, backup):
        self.context["backup"] = backup

###############################################################################
# BackupFinishedEvent
###############################################################################
class BackupFinishedEvent(BackupEvent):
    """
    Base class for all backup events
    """
    ###########################################################################
    def __init__(self, backup=None, state=None):
        super(BackupFinishedEvent, self).__init__(backup=backup)
        self.state = state
        self.event_type = BackupEventTypes.BACKUP_FINISHED

    ###########################################################################
    @property
    def state(self):
        return self.context.get("state")

    @state.setter
    def state(self, state):
        self.context["state"] = state

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(BackupFinishedEvent, self).to_document(display_only=display_only)
        doc.update({
            "_type": "BackupFinishedEvent"
        })

        return doc
