__author__ = 'abdul'

from base import MBSObject
from globals import State




###############################################################################
##############################                 ################################
############################## Audit Entities  ################################
##############################                 ################################
###############################################################################



###############################################################################
# AuditReport
###############################################################################
class AuditReport(MBSObject):
    ###########################################################################
    def __init__(self):
        self._id = None
        self._audit_type = None
        self._audit_date = None
        self._failed_audits = []
        self._warned_audits = []
        self._total_audits = 0
        self._total_success = 0
        self._total_failures = 0
        self._total_warnings = 0

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
    def failed_audits(self):
        return self._failed_audits


    @failed_audits.setter
    def failed_audits(self, failed_audits):
        self._failed_audits = failed_audits

    ###########################################################################
    @property
    def warned_audits(self):
        return self._warned_audits


    @warned_audits.setter
    def warned_audits(self, warned_audits):
        self._warned_audits = warned_audits

    ###########################################################################
    def has_failures(self):
        return self.failed_audits is not None and self.failed_audits

    ###########################################################################
    def has_warnings(self):
        return self.warned_audits is not None and self.warned_audits

    ###########################################################################
    @property
    def total_audits(self):
        return self._total_audits


    @total_audits.setter
    def total_audits(self, total_audits):
        self._total_audits = total_audits

    ###########################################################################
    @property
    def total_success(self):
        return self._total_success


    @total_success.setter
    def total_success(self, total_success):
        self._total_success = total_success

    ###########################################################################
    @property
    def total_failures(self):
        return self._total_failures


    @total_failures.setter
    def total_failures(self, total_failures):
        self._total_failures = total_failures

    ###########################################################################
    @property
    def total_warnings(self):
        return self._total_warnings


    @total_warnings.setter
    def total_warnings(self, total_warnings):
        self._total_warnings = total_warnings

    ###########################################################################
    def to_document(self, display_only=False):
        return {
            "_type": "AuditReport",
            "auditType": self.audit_type,
            "auditDate": self.audit_date,
            "failures": self._export_failures(display_only=display_only),
            "warnings": self._export_warnings(display_only=display_only),
            "totalAudits": self.total_audits,
            "totalSuccess": self.total_success,
            "totalFailures": self.total_failures,
            "totalWarnings": self.total_warnings,
            }

    ###########################################################################
    def _export_failures(self, display_only=False):
        return map(lambda entry: entry.to_document(display_only=display_only),
                   self.failed_audits)

    ###########################################################################
    def _export_warnings(self, display_only=False):
        return map(lambda entry: entry.to_document(display_only=display_only),
                    self.warned_audits)

    ###########################################################################
    def summary(self):
        lines = []
        lines.append("Report ID: %s" % self.id)
        lines.append("Audit Date: %s" % self.audit_date)
        lines.append("Total Audits: %s" % self.total_audits)
        lines.append("Total Success: %s" % self.total_success)
        lines.append("Total Failed: %s" % self.total_failures)
        lines.append("Total Warnings: %s" % self.total_warnings)
        return "\n".join(lines)

###############################################################################
# AuditEntry
###############################################################################

class AuditEntry(MBSObject):

    ###########################################################################
    def __init__(self):
        self._state = None
        self._backup_id = None
        self._errors = None
        self._warnings = None

    ###########################################################################
    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

    ###########################################################################
    def failed(self):
        return not self.succeeded()

    ###########################################################################
    def warned(self):
        return self.warnings is not None and len(self.warnings) > 0

    ###########################################################################
    def succeeded(self):
        return self.state == State.SUCCEEDED

    ###########################################################################
    @property
    def backup_id(self):
        return self._backup_id

    @backup_id.setter
    def backup_id(self, backup_id):
        self._backup_id = backup_id

    ###########################################################################
    @property
    def errors(self):
        return self._errors

    @errors.setter
    def errors(self, errors):
        self._errors = errors

    ###########################################################################
    @property
    def warnings(self):
        return self._warnings

    @warnings.setter
    def warnings(self, warnings):
        self._warnings = warnings

    ###########################################################################
    def _export_errors(self):
        return self._export_logs(self.errors)

    ###########################################################################
    def _export_warnings(self):
        return self._export_logs(self.errors)

    ###########################################################################
    def _export_logs(self, logs):
        result = []

        for log_entry in logs:
            result.append(log_entry.to_document())

        return result

    ###########################################################################
    def to_document(self, display_only=False):
        doc= {
            "state": self.state,

        }

        if self.backup_id:
            doc["backupId"] = self.backup_id
        if self.errors:
            doc["errors"] = self._export_errors()
        if self.warnings:
            doc["warnings"] = self._export_warnings()

        return doc



###############################################################################
# PlanScheduleAuditReport
###############################################################################
class PlanScheduleAuditReport(AuditReport):

    ###########################################################################
    def __init__(self):
        AuditReport.__init__(self)

    ###########################################################################
    def to_document(self, display_only=False):
        doc = super(PlanScheduleAuditReport, self).to_document(
            display_only=display_only)
        doc["_type"] = "PlanScheduleAuditReport"
        return doc

    ###########################################################################
    def summary(self):
        lines = []
        lines.append("Report ID: %s" % self.id)
        lines.append("Audit Date: %s" % self.audit_date)
        lines.append("Total # of Plans Audited: %s" % self.total_audits)
        lines.append("Total Success: %s" % self.total_success)
        lines.append("Total Failures: %s" % self.total_failures)
        lines.append("Total Warnings: %s" % self.total_warnings)

        lines.append("\n\n")

        lines.append("Failure Summary")

        formatter = "%-25s %s"
        header = formatter % ("Plan ID", "Failed Occurrences/Error")
        lines.append(header)

        def extract_failed_occ(plan_audit_entry):
            return "%s : %s " % (plan_audit_entry.plan_occurrence,
                                 plan_audit_entry.state)

        for failed_plan_report in self.failed_audits:
            plan_id = failed_plan_report.plan.id
            failed_occurrences = map(extract_failed_occ,
                                     failed_plan_report.failed_audits)

            lines.append(formatter % (plan_id, failed_occurrences))

        return "\n".join(lines)



###############################################################################
# PlanAuditReport
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
    def to_document(self, display_only=False):
        doc = super(PlanAuditReport, self).to_document(display_only=
                                                       display_only)
        doc["plan"] = self.plan.to_document(display_only=display_only)
        doc["_type"] = "PlanAuditReport"
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
    def to_document(self, display_only=False):
        doc = super(PlanAuditEntry, self).to_document(display_only=display_only)
        doc["planOccurrence"] = self.plan_occurrence

        return doc