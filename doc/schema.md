# New backup system data model


### BackupPlan

* A description of backup plan

```
{
    "_type": "BackupPlan",
    "_id": <string>,
    "description": <string>,
    "source": <BackupSource>,
    "target": <BackupTarget>,
    "schedule": {
        "frequencyInSeconds": <int>,
        "offset": <date>
    },
    "nextOccurrence": <date>,
    ["generator": <string>,]
    "strategy": <string>, // TB Reconsidered ("DUMP" | "EBS_SNAPSHOT" | "DB_FILES")
    "backupType": None,//TBD
    ["retentionPolicy": <RetentionPolicy>],
    ["tags: [<string>,...], ]
    ["backupNamingScheme: <string> | <BackupNamingScheme>, ] // template string or BackupNamingScheme document
    ["primaryOk": <boolean>, ]
}
```

### RetentionPolicy

* A description of a backup file retention policy
* Has multiple types

```
// RetainLastNPolicy
{
    "_type": "RetainLastNPolicy",
    "retainCount": <int>
}

// RetainMaxTimePolicy
{
    "_type": "RetainMaxTimePolicy",
    "maxTime": <int>  // seconds
}

```

### BackupNamingScheme

* A description of a backup file naming scheme
* Has multiple types

```
TBD

### Backup

* Represents an execution of a single backup

```
{
    "_type": "Backup",
    "_id": <string>,
    ["plan": <BackupPlan>,] // One off backups wont have plans
    ["planOccurrence": <timestamp>,] 
    "source": <BackupSource>,
    ["sourceStats": {
         "_type": "SourceStats",
         "optime": <timestamp>,
         "replLag": <int>
     },]
    "engineGuid": <string>,
    "target": <BackupTarget>,
    "targetReference": <TargetReference>,
    "state": <string>, // ("SCHEDULED" | "IN_PROGRESS" | "SUCCEEDED" | "FAILED" | "CANCELED"),
    "logs": [
            {

                ["name": <string>,] // TB Reconsidered
                "date": <date>,
                "state": <string>, // ("SCHEDULED" | "IN_PROGRESS" | "SUCCEEDED" | "FAILED" | "CANCELED")
                "level": <string>. // ("INFO" | "WARNING" | "ERROR" )
                "message": <string>,
            },
            ....
        ],
    "backupRateInMB": <float> ,
    "startDate": <date>
    "endDate": <date>,
    ["tags": [<string>, ...] ,]
}
```

### BackupSource

* A description of the backup source
* Has multiple types
* Properties differ depending on type

```
// MongoSource
{
    "_type": "MongoSource",
    "uri": <string>, // supports cluster, server,or db uris
}

// MongoLabServerSource
{
    "_type": "MongoLabServerSource",
    "serverId": <string>,
}

// MongoLabClusterSource
{
    "_type": "MongoLabClusterSource",
    "clusterId": <string>,
}

// MongoLabDatabaseSource
{
    "_type": "MongoLabDatabaseSource",
    "databaseId": <string>,
}

// EbsVolumeSource
{
    "_type": "EbsVolumeSource",
    "volumeId": <string>,
    "accessKey": <string>,
    "secretKey": <string>
}



```

### BackupTarget

* A description of a backup target
* Has multiple types
* Properties differ depending on type

```
// S3BucketTarget
{
    "_type": "S3BucketTarget",
    "bucketName": <string>,
    "accessKey": <string>,
    "secretKey": <string>
}

// EbsSnapshotTarget
{
    "_type": "EbsSnapshotTarget",
    "accessKey": self.access_key,
    "secretKey": self.secret_key
}
```

```

### TargetReference

* A reference to the backup resource that was saved to the target
* Every target on put returns a target reference to the resource
* Currently has the following implementations
```
// FileReference
{
    "_type": "FileReference",
    "fileName": <string>,
    "fileSize": <long> // in bytes
}

// EbsSnapshotReference
{
    "_type": "EbsSnapshotReference",
    "snapshotId": <string>
}
```
