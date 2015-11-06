__author__ = 'abdul'

import logging
import time

from base import MBSObject
from threading import Thread

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

    ####################################################################################################################
    def create_event(self, event):
        self._event_collection.save_document(event.to_document())

    ####################################################################################################################
    def register_event_listener(self, event_listener):
        existing = self._event_listener_collection.find_one({
            "name": event_listener.name
        })

        if existing:
            event_listener.id = existing.id
            event_listener.last_seen_date = existing.last_seen_date
        else:
            listener_doc = event_listener.to_document()
            self._event_listener_collection.save_document(listener_doc)
            event_listener.id = listener_doc["_id"]

        # start listening to events
        EventNotifier(self, event_listener).start()

    ####################################################################################################################
    def listen_to_events(self, event_listener):
        # TODO XXX we have to fix maker to work with tailable cursors
        # currently we grab the raw dict from the pymongo collection and make it manually
        q = None
        if event_listener.last_seen_date:
            q = {
                "createdDate": {
                    "$gt": event_listener.last_seen_date
                }
            }
        cursor = self._event_collection.collection.find(query=q, tailable=True, await_data=True)
        while cursor.alive:
            try:
                event_doc = cursor.next()
                event = self._event_collection.make_obj(event_doc)
                event_listener.handle_event(event)
                self.update_listener_last_seen(event_listener, event)
            except StopIteration:
                time.sleep(1)

    ####################################################################################################################
    def notify_listener(self, event_listener, event):
        event_listener.handle_event(event)
        self.update_listener_last_seen(event_listener, event)

    ####################################################################################################################
    def update_listener_last_seen(self, event_listener, event):
        event_listener.last_seen_date = event.created_date
        q = {"_id": event_listener.id}
        u = {
            "$set": {
                "lastSeenDate": event_listener.last_seen_date
            }
        }
        self._event_listener_collection.update(q, u)


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
        self._last_seen_date = None

    ####################################################################################################################
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    ####################################################################################################################
    @property
    def last_seen_date(self):
        return self._last_seen_date

    @last_seen_date.setter
    def last_seen_date(self, value):
        self._last_seen_date = value

    ####################################################################################################################
    def handle_event(self, event):
        pass

    ####################################################################################################################
    def to_document(self, display_only=False):
        doc = super(EventListener, self).to_document(display_only=display_only)
        doc.update({
            "name": self.name,
            "lastSeenDate": self.last_seen_date
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
            "context": self._export_context(display_only=display_only)
        })

        return doc