__author__ = 'abdul'

TYPE_BINDINGS = {
    "Backup": "mbs.backup.Backup",
    "BackupLogEntry": "mbs.backup.BackupLogEntry",
    "Plan": "mbs.plan.BackupPlan",
    "Schedule": "mbs.plan.Schedule",
    "Source": "mbs.source.BackupSource",
    "Target": "mbs.target.BackupTarget",
    "TargetReference": "mbs.target.TargetReference",
    "ServerSource": "mbs.source.ServerSource",
    "DatabaseSource": "mbs.source.DatabaseSource",
    "ClusterSource": "mbs.source.ClusterSource",
    "EbsVolumeSource": "mbs.source.EbsVolumeSource",
    "S3BucketTarget": "mbs.target.S3BucketTarget",
    "EbsSnapshotTarget": "mbs.target.EbsSnapshotTarget",
    "FileTargetReference": "mbs.target.FileTargetReference",
    "EbsSnapshotReference": "mbs.target.EbsSnapshotReference",
    "AuditReport": "mbs.audit.AuditReport",
    "AuditEntry": "mbs.audit.AuditEntry",
    "PlanAuditReport": "mbs.audit.PlanAuditReport",
    "PlanAuditEntry": "mbs.audit.PlanAuditEntry"
}
