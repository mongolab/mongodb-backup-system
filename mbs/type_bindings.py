__author__ = 'abdul'

TYPE_BINDINGS = {
    "PlanManager": "mbs.manager.PlanManager",
    "BackupEngine": "mbs.engine.BackupEngine",
    "Backup": "mbs.backup.Backup",
    "BackupLogEntry": "mbs.backup.BackupLogEntry",
    "Plan": "mbs.plan.BackupPlan",
    "Schedule": "mbs.plan.Schedule",
    "Strategy": "mbs.strategy.BackupStrategy",
    "DumpStrategy": "mbs.strategy.DumpStrategy",
    "Source": "mbs.source.BackupSource",
    "SourceStats": "mbs.source.SourceStats",
    "Target": "mbs.target.BackupTarget",
    "TargetReference": "mbs.target.TargetReference",
    "MongoSource": "mbs.source.MongoSource",
    "EbsVolumeSource": "mbs.source.EbsVolumeSource",
    "S3BucketTarget": "mbs.target.S3BucketTarget",
    "EbsSnapshotTarget": "mbs.target.EbsSnapshotTarget",
    "RackspaceCloudFilesTarget": "mbs.target.RackspaceCloudFilesTarget",
    "FileReference": "mbs.target.FileReference",
    "EbsSnapshotReference": "mbs.target.EbsSnapshotReference",
    "RetainLastNPolicy": "mbs.policies.RetainLastNPolicy",
    "RetainMaxTimePolicy": "mbs.policies.RetainMaxTimePolicy",
    "AuditReport": "mbs.audit.AuditReport",
    "AuditEntry": "mbs.audit.AuditEntry",
    "PlanAuditReport": "mbs.audit.PlanAuditReport",
    "PlanAuditEntry": "mbs.audit.PlanAuditEntry",
    "EmailNotificationHandler": "mbs.notification.EmailNotificationHandler",
    "Encryptor": "mbs.encryption.Encryptor"

}
