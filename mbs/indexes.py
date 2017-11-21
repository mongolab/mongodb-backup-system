__author__ = 'abdul'

from pymongo import ASCENDING, DESCENDING

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
        },

        {
            "index": [
                ('engineGuid', ASCENDING),
                ('state', ASCENDING),
                ('cancelRequestedAt', ASCENDING)
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

    "audits": [
        {
            "index": [('auditDate', ASCENDING), ('auditType', ASCENDING)]
        }
    ]
}



