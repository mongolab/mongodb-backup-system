__author__ = 'abdul'

from pymongo import ASCENDING

MBS_INDEXES = {
    "backups": [
        [('state', ASCENDING), ('plan.nextOccurrence', ASCENDING)],
        [('planOccurrence', ASCENDING), ('plan._id', ASCENDING)],
        [('state', ASCENDING), ('plan.$id', ASCENDING)],
        [('state', ASCENDING), ('engineGuid', ASCENDING)],
        [('plan.description', ASCENDING)]
    ],

    "plans":[
        [('source._type', ASCENDING)]
    ]
}



