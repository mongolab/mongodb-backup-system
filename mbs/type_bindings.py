__author__ = 'abdul'

TYPE_BINDINGS = {
    "BackupSystem": "mbs.backup_system.BackupSystem",
    "BackupSystemApiServer": "mbs.api.BackupSystemApiServer",
    "BackupEngine": "mbs.engine.BackupEngine",
    "Backup": "mbs.backup.Backup",
    "Restore": "mbs.restore.Restore",
    #TODO Completely remove BackupLogEntry because it was changed to EventLogEntry
    "BackupLogEntry": "mbs.task.EventLogEntry",
    "EventLogEntry": "mbs.task.EventLogEntry",
    "Plan": "mbs.plan.BackupPlan",
    "Schedule": "mbs.schedule.Schedule",
    "CronSchedule": "mbs.schedule.CronSchedule",
    "Strategy": "mbs.strategy.BackupStrategy",
    "DumpStrategy": "mbs.strategy.DumpStrategy",
    "CloudBlockStorageStrategy": "mbs.strategy.CloudBlockStorageStrategy",
    "EbsVolumeStorageStrategy": "mbs.strategy.EbsVolumeStorageStrategy",
    "HybridStrategy": "mbs.strategy.HybridStrategy",
    "DataSizePredicate": "mbs.strategy.DataSizePredicate",
    "Source": "mbs.source.BackupSource",
    "SourceStats": "mbs.source.SourceStats",
    "Target": "mbs.target.BackupTarget",
    "TargetReference": "mbs.target.TargetReference",
    "MongoSource": "mbs.source.MongoSource",
    "EbsVolumeStorage": "mbs.source.EbsVolumeStorage",
    "S3BucketTarget": "mbs.target.S3BucketTarget",
    "RackspaceCloudFilesTarget": "mbs.target.RackspaceCloudFilesTarget",
    "FileReference": "mbs.target.FileReference",
    "EbsSnapshotReference": "mbs.target.EbsSnapshotReference",
    "RetainLastNPolicy": "mbs.retention.RetainLastNPolicy",
    "RetainMaxTimePolicy": "mbs.retention.RetainMaxTimePolicy",
    "PlanScheduleAuditor": "mbs.auditors.PlanScheduleAuditor",
    "PlanRetentionAuditor": "mbs.auditors.PlanRetentionAuditor",
    "AuditReport": "mbs.audit.AuditReport",
    "AuditEntry": "mbs.audit.AuditEntry",
    "PlanAuditReport": "mbs.audit.PlanAuditReport",
    "PlanScheduleAuditReport": "mbs.audit.PlanScheduleAuditReport",
    "PlanAuditEntry": "mbs.audit.PlanAuditEntry",
    "EmailNotificationHandler": "mbs.notification.EmailNotificationHandler",
    "Encryptor": "mbs.encryption.Encryptor",
    "SourceIPTag": "mbs.tags.SourceIPTag",
    "DefaultBackupNamingScheme": "mbs.naming_scheme.DefaultBackupNamingScheme",
    "TemplateBackupNamingScheme": "mbs.naming_scheme.TemplateBackupNamingScheme",
    "BackupSweeper": "mbs.retention.BackupSweeper",
    "BackupExpirationManager": "mbs.retention.BackupExpirationManager"

}
