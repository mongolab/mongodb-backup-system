# New backup system data model


### BackupPlan

* A description of backup plan

```
{
    "_id": <string>,
    "source": <BackupSource>,
    "target": <BackupTarget>,
    "schedule": {
        "frequency": <int>,
        "offset": <date>
    },
    "nextOccurrence": <date>,
    "backupType": None,//TBD
    "retentionPolicy": None //TBD
}
```

### Backup

* Represents an execution of a single backup

```
{
    "_id": <string>,
    ["plan": <BackupPlan>,] // One off backups wont have plans
    ["planOccurrence": <timestamp>,] 
    "source": <BackupSource>,
    "sourceOptime": <timestamp>,
    "engineId": <string>,
    "target": <BackupTarget>,
    "state": <string>, // ("SCHEDULED" | "IN_PROGRESS" | "SUCCEEDED" | "FAILED" | "CANCELED"),
    "logs": [
            {
                "date": <date>,
                "state": <string>, // ("SCHEDULED" | "IN_PROGRESS" | "SUCCEEDED" | "FAILED" | "CANCELED")
                "level": <string>. // ("INFO" | "WARNING" | "ERROR" )
                "message": <string>,
            },
            ....
        ]
}
```

### BackupSource

* A description of the backup source
* Has multiple types
* Properties differ depending on type

```
// ServerSource
{
    "_type": "backup.backup_new.ServerSource",
    "address": <string>, // "host:port"
    "adminUsername": <string>,
    "adminPassword": <string>
}

// MongoLabServerSource
{
    "_type": "backup.backup_new.MongoLabServerSource",
    "serverId": <string>
}

// MongoLabClusterSource
{
    "_type": "backup.backup_new.MongoLabClusterSource",
    "clusterId": <string>
}

// Database Source

{
    "_type": "backup.backup_new.DatabaseSource",
    "databaseURI": <string> // mongodb uri
}

// Hosted Database Source

{
    "_type": "backup.backup_new.HostedDatabaseSource",
    "hostedDatabaseId": <string>
}

```

### BackupTarget

* A description of a backup target
* Has multiple types
* Properties differ depending on type

```
{
    "_type": "backup.backup_new.S3BucketTarget",
    "bucketName": <string>,
    "accessKey": <string>,
    "secretKey": <string>
}
```
