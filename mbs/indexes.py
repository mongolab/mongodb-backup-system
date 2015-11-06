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
            "index": [('state', ASCENDING), ('engineGuid', ASCENDING), ('plan.nextOccurrence', ASCENDING)]
        },
        {
            "index": [
                ('deletedDate', DESCENDING)
            ],
            "args": {
                "expireAfterSeconds": SIX_MONTH_SECONDS
            }
        },
        {
            "index": [
                ('state', ASCENDING),
                ('dontExpire', ASCENDING),
                ('plan._id', DESCENDING),
                ('expiredDate', DESCENDING),
                ('createdDate', DESCENDING)
            ]
        },
        {
            "index": [
                ('state', ASCENDING),
                ('dontExpire', ASCENDING),
                ('expiredDate', DESCENDING)
            ]
        },

        {
            "index": [
                ('deletedDate', DESCENDING),
                ('expiredDate', DESCENDING)
            ]
        }
    ],

    "plans": [
        {
            "index": [('nextOccurrence', ASCENDING), ('priority', ASCENDING)]
        }
    ],

    "restores": [
        {
            "index": [('state', ASCENDING), ('engineGuid', ASCENDING)]
        }
    ],

    "events": [
        {
            "index": [('createdDate', ASCENDING)]
        }
    ],

    "event-listeners": [
        {
            "index": [('name', ASCENDING)],
            "args": {
                "unique": True
            }
        }
    ]
}



