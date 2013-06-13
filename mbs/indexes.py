__author__ = 'abdul'

from pymongo import ASCENDING, DESCENDING

SIX_MONTH_SECONDS = 60 * 60 * 24 * 30 * 6

MBS_INDEXES = {
    "backups": [
            {
            "index": [('state', ASCENDING), ('plan.nextOccurrence', ASCENDING)]
        },
            {
            "index": [('planOccurrence', ASCENDING), ('plan._id', ASCENDING)]
        },
            {
            "index": [('state', ASCENDING), ('plan.$id', ASCENDING)]
        },
            {
            "index": [('state', ASCENDING), ('engineGuid', ASCENDING)]
        },
            {
            "index": [('plan.description', ASCENDING)]
        },
            {
            "index": [('targetReference.expiredDate', ASCENDING)],
            "args": {
                "expireAfterSeconds": SIX_MONTH_SECONDS
            }
        } ,

            {
            "index": [ ('plan._id', ASCENDING), ('targetReference', ASCENDING),
                       ('createdDate', DESCENDING),
                       ('targetReference.expiredDate', ASCENDING) ]
        }
    ],

    "plans":[
        {
            "index":[('source._type', ASCENDING)]
        }
    ],

    "restores":[
            {
            "index": [('state', ASCENDING), ('engineGuid', ASCENDING)]
        }
    ]
}



