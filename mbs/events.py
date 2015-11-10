__author__ = 'abdul'

import logging
import time

from base import MBSObject
from threading import Thread

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
    def __init__(self, event_collection, event_listener_collection):
        self._event_collection = event_collection
        self._event_listener_collection = event_listener_collection
        # lazy loaded cache
        self._event_listener_cache = None

    ####################################################################################################################
    def event_listeners(self):
        if not self._event_listener_cache:
            self._event_listener_cache = SimpleCache(default_timeout=5*60)

        listeners = self._event_listener_cache.get("eventListeners")
        if listeners is None:
            listeners = list(self._event_listener_collection.find())
            self._event_listener_cache.set("eventListeners", listeners)

        return listeners

    ####################################################################################################################
    def create_event(self, event):
        print "Creating event: %s. event created date:%s" % (event, event.created_date)
        self._populate_listener_subscriptions(event)
        self._event_collection.save_document(event.to_document())

    ####################################################################################################################
    def _populate_listener_subscriptions(self, event):
        subscriptions = []
        for event_listener in self.event_listeners():
            if not event_listener.event_types or event.event_type in event_listener.event_types:
                subscriptions.append({
                    "name": event_listener.name,
                    "acknowledged": False
                })
        event.subscribed_event_listeners = subscriptions

    ####################################################################################################################
    def register_event_listener(self, event_listener):
        existing = self._event_listener_collection.find_one({
            "name": event_listener.name
        })

        if not existing:
            listener_doc = event_listener.to_document()
            self._event_listener_collection.save_document(listener_doc)
            event_listener.id = listener_doc["_id"]
        else:
            # update
            pass

        # start an event notifier
        EventNotifier(self, event_listener).start()

    ####################################################################################################################
    def listen_to_events(self, event_listener):
        # TODO XXX we have to fix maker to work with tailable cursors
        # currently we grab the raw dict from the pymongo collection and make it manually
        q = {
            "subscribedEventListeners": {
                "$elemMatch": {
                    "name": event_listener.name,
                    "acknowledged": False
                }
            }
        }

        while True:
            cursor = self._event_collection.collection.find(query=q, tailable=True, await_data=True)

            while cursor.alive:
                try:
                    event_doc = cursor.next()
                    event = self._event_collection.make_obj(event_doc)
                    if self._is_event_subscribed_listener(event, event_listener):
                        event_listener.handle_event(event)
                        self.acknowledge_event_by_listener(event_listener, event)
                except StopIteration:
                    time.sleep(1)

    ####################################################################################################################
    def _is_event_subscribed_listener(self, event, event_listener):
        return len(filter(lambda s: s["name"] == event_listener.name, event.subscribed_event_listeners)) > 0

    ####################################################################################################################
    def acknowledge_event_by_listener(self, event_listener, event):
        q = {
            "_id": event.id,
            "subscribedEventListeners": {
                "$elemMatch": {
                    "name": event_listener.name,
                    "acknowledged": False
                }
            }
        }

        u = {
            "$set": {
                "subscribedEventListeners.$.acknowledged": True
            }
        }

        self._event_collection.update(q, u)

########################################################################################################################
# EventNotifier
########################################################################################################################
class EventNotifier(Thread):

    ####################################################################################################################
    def __init__(self, event_queue, event_listener):
        Thread.__init__(self)
        self.daemon = True
        self._event_queue = event_queue
        self._event_listener = event_listener

    ####################################################################################################################
    def run(self):
        self._event_queue.listen_to_events(self._event_listener)

########################################################################################################################
# EventListener
########################################################################################################################
class EventListener(MBSObject):

    ####################################################################################################################
    def __init__(self):
        super(EventListener, self).__init__()
        self._name = None
        self._event_types = None

    ####################################################################################################################
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    ####################################################################################################################
    @property
    def event_types(self):
        return self._event_types

    @event_types.setter
    def event_types(self, value):
        self._event_types = value

    ####################################################################################################################
    def handle_event(self, event):
        pass

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(EventListener, self).to_document(display_only=display_only)
        doc.update({
            "name": self.name,
            "eventTypes:": self.event_types
        })
        return doc

########################################################################################################################
# Event
########################################################################################################################
class Event(MBSObject):

    ####################################################################################################################
    def __init__(self):
        super(Event, self).__init__()
        self._event_type = None
        self._created_date = None
        self._context = {}
        self._subscribed_event_listeners = {}

    ####################################################################################################################
    @property
    def event_type(self):
        return self._event_type

    @event_type.setter
    def event_type(self, value):
        self._event_type = value

    ####################################################################################################################
    @property
    def created_date(self):
        return self._created_date

    @created_date.setter
    def created_date(self, value):
        self._created_date = value

    ####################################################################################################################
    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, value):
        self._context = value

    ####################################################################################################################
    @property
    def subscribed_event_listeners(self):
        return self._subscribed_event_listeners

    @subscribed_event_listeners.setter
    def subscribed_event_listeners(self, value):
        self._subscribed_event_listeners = value

    ####################################################################################################################
    def _export_context(self, display_only=False):
        exported_context = {}
        if self.context:
            for name,value in self.context.items():
                if isinstance(value, MBSObject):
                    exported_context[name] = value.to_document(display_only=display_only)
                else:
                    exported_context[name] = value

        return exported_context

    ####################################################################################################################
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

########################################################################################################################
# Backup Event Types

class BackupEventTypes(object):
    BACKUP_FINISHED = "BackupFinished"

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
        self.backup = backup

    ####################################################################################################################
    @property
    def backup(self):
        return self.context.get("backup")

    @backup.setter
    def backup(self, backup):
        self.context["backup"] = backup

########################################################################################################################
# BackupFinishedEvent
########################################################################################################################
class BackupFinishedEvent(BackupEvent):
    """
    Base class for all backup events
    """
    ####################################################################################################################
    def __init__(self, backup=None, state=None):
        super(BackupFinishedEvent, self).__init__(backup=backup)
        self.state = state
        self.event_type = BackupEventTypes.BACKUP_FINISHED

    ####################################################################################################################
    @property
    def state(self):
        return self.context.get("state")

    @state.setter
    def state(self, state):
        self.context["state"] = state

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(BackupFinishedEvent, self).to_document(display_only=display_only)
        doc.update({
            "_type": "BackupFinishedEvent"
        })

        return doc
