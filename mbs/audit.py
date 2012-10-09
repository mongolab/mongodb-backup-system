__author__ = 'abdul'

from mbs.backup import STATE_SUCCEEDED

from mbs.utils import yesterday_date, document_pretty_string

###############################################################################
##############################                 #################################
############################## Backup Auditors #################################
##############################                 #################################
###############################################################################

TYPE_PLAN_AUDIT = "PLAN_AUDIT"

###############################################################################
# BackupAuditor
# Creates an audit report about backups taken as of a specific day.
#
class BackupAuditor(object):

    ###########################################################################
    def __init__(self, audit_type):
        self._audit_type = audit_type

    ###########################################################################
    def daily_audit_reports(self, audit_date):
        pass

    ###########################################################################
    def yesterday_audit_reports_as_of(self):
        return self.daily_audit_report(yesterday_date())


    ###########################################################################
    @property
    def audit_type(self):
        return self._audit_type

###############################################################################
# AuditReport
###############################################################################
class AuditReport(object):
    ###########################################################################
    def __init__(self):
        self._id = None
        self._audit_type = None
        self._audit_date = None
        self._audit_entries = []

    ###########################################################################
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = str(id)

    ###########################################################################
    @property
    def audit_type(self):
        return self._audit_type


    @audit_type.setter
    def audit_type(self, audit_type):
        self._audit_type = audit_type

    ###########################################################################
    @property
    def audit_date(self):
        return self._audit_date


    @audit_date.setter
    def audit_date(self, audit_date):
        self._audit_date = audit_date

    ###########################################################################
    @property
    def audit_entries(self):
        return self._audit_entries


    @audit_entries.setter
    def audit_entries(self, audit_entries):
        self._audit_entries = audit_entries

    ###########################################################################
    def to_document(self):
        doc = {
            "_type": "AuditReport",
            "auditType": self.audit_type,
            "auditDate": self.audit_date,
            "auditEntries": self._export_audit_entries(),
            }

        if self.id:
            doc["_id"] = self.id

        return doc

    ###########################################################################
    def _export_audit_entries(self):
        return map(lambda entry: entry.to_document(), self.audit_entries)

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())

###############################################################################
# AuditEntry
###############################################################################
class AuditEntry(object):

    ###########################################################################
    def __init__(self):
        self._is_backed_up = None
        self._backup_record = None

    ###########################################################################
    @property
    def is_backed_up(self):
        return self._is_backed_up

    @is_backed_up.setter
    def is_backed_up(self, is_backed_up):
        self._is_backed_up = is_backed_up

    ###########################################################################
    @property
    def backup_record(self):
        return self._backup_record

    ###########################################################################
    @backup_record.setter
    def backup_record(self, backup):
        self._backup_record = backup
    ###########################################################################
    def to_document(self):
        pass

    ###########################################################################
    def __str__(self):
        return document_pretty_string(self.to_document())

###############################################################################
# PlanBackupAuditor
# Creates an audit report about backup plans taken yesterday.

class PlanAuditor(BackupAuditor):
    ###########################################################################
    def __init__(self,
                 plan_collection,
                 backup_collection):

        BackupAuditor.__init__(self, TYPE_PLAN_AUDIT)

        self._plan_collection = plan_collection
        self._backup_collection = backup_collection


    ###########################################################################
    # plan auditing
    ###########################################################################
    def daily_audit_reports(self, audit_date):

        reports = []

        for plan in self._plan_collection.find():
            report = PlanAuditReport()
            report.audit_type = self.audit_type
            report.audit_date = audit_date
            report.plan = plan
            report.audit_entries = self._audit_plan(plan, audit_date)
            reports.append(report)

        return reports

    ###########################################################################
    def _audit_plan(self, plan, audit_date):
        audit_entries = []

        for plan_occurrence in plan.natural_occurrences_as_of(audit_date):
            audit_entry = self._audit_plan_occurrence(plan, plan_occurrence)
            audit_entries.append(audit_entry)

        return audit_entries

    ###########################################################################
    def _audit_plan_occurrence(self, plan, plan_occurrence):
        backup_record = self._lookup_backup_by_plan_occurrence(plan,
                                                               plan_occurrence)

        audit_entry = PlanAuditEntry()
        audit_entry.backup_record = backup_record
        audit_entry.is_backed_up = backup_record is not None
        audit_entry.plan_occurrence = plan_occurrence

        return audit_entry

    ###########################################################################
    def _lookup_backup_by_plan_occurrence(self, plan, plan_occurrence):

        q = {
            "state": STATE_SUCCEEDED,
            "plan._id": plan._id,
            "planOccurrence":plan_occurrence,
            }
        c = self._backup_collection

        return c.find_one(q)


###############################################################################
# PlanAuditEntry
###############################################################################
class PlanAuditReport(AuditReport):

    ###########################################################################
    def __init__(self):
        AuditReport.__init__(self)
        self._plan = None

    ###########################################################################
    @property
    def plan(self):
        return self._plan

    @plan.setter
    def plan(self, plan):
        self._plan = plan

    ###########################################################################
    def to_document(self):
        doc = super(PlanAuditReport, self).to_document()
        doc["plan"] = self.plan.to_document()
        return doc

###############################################################################
# PlanAuditEntry
###############################################################################
class PlanAuditEntry(AuditEntry):

    ###########################################################################
    def __init__(self):
        AuditEntry.__init__(self)
        self._plan_occurrence = None

    ###########################################################################
    @property
    def plan_occurrence(self):
        return self._plan_occurrence

    @plan_occurrence.setter
    def plan_occurrence(self, plan_occurrence):
        self._plan_occurrence = plan_occurrence

    ###########################################################################
    def to_document(self):
        backup_doc = (self.backup_record.to_document() if self._backup_record
                      else None)
        return {
            "_type": "PlanAuditEntry",
            "backupRecord": backup_doc,
            "isBackedUp": self.is_backed_up,
            "planOccurrence": self.plan_occurrence
        }

###############################################################################
class GlobalAuditor():

    ###########################################################################
    def __init__(self, audit_collection):
        self._auditors = []
        self._audit_collection = audit_collection

    ###########################################################################
    def register_auditor(self, auditor):
        self._auditors.append(auditor)

    ###########################################################################
    def generate_daily_audit_reports(self, date):
        for auditor in self._auditors:
            reports = auditor.daily_audit_reports(date)
            for report in reports:
                self._audit_collection.save_document(report.to_document())

    ###########################################################################
    def generate_yesterday_audit_reports(self):
        self.generate_daily_audit_reports(yesterday_date())

    ###########################################################################