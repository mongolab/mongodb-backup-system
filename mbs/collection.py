__author__ = 'abdul'

from globals import EventType
from utils import listify
from makerpy.object_collection import ObjectCollection
from mongo_utils import objectiditify


###############################################################################
# MBSObjectCollection class
###############################################################################
class MBSObjectCollection(ObjectCollection):
    ###########################################################################
    def __init__(self, collection, clazz=None, type_bindings=None):
        # call super
        ObjectCollection.__init__(self, collection, clazz=clazz,
                                  type_bindings=type_bindings)

    ###########################################################################
    def get_by_id(self, object_id):
        object_id = objectiditify(object_id)
        q = {
            "_id": object_id
        }
        return self.find_one(q)

    ###########################################################################
    def remove_by_id(self, object_id):
        object_id = objectiditify(object_id)
        return ObjectCollection.remove_by_id(self, object_id)

###############################################################################
# MBSTaskCollection class
###############################################################################
class MBSTaskCollection(MBSObjectCollection):
    ###########################################################################
    def __init__(self, collection, clazz=None, type_bindings=None):
        # call super
        MBSObjectCollection.__init__(self, collection, clazz=clazz,
                                     type_bindings=type_bindings)

    ###########################################################################
    def update_task(self, task, properties=None, event_name=None,
                    event_type=EventType.INFO, message=None, details=None):
        """
            Updates the specified properties of the specified MBSTask object
        """
        task_doc = task.to_document()
        q = {
            "_id": task.id
        }

        u = {}

        # log the event as needed
        if event_name or message:
            log_entry = task.log_event(name=event_name, event_type=event_type,
                                       message=message, details=details)
            # push if "logs" property is not included
            if not (properties and "logs" in properties):
                u["$push"] = {"logs": log_entry.to_document()}

        # construct $set operator
        if properties:
            properties = listify(properties)
            u["$set"] = {}
            for prop in properties:
                u["$set"][prop] = task_doc.get(prop)

        self.update(spec=q, document=u)
