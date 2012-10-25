__author__ = 'abdul'

from base import MBSObject
from backup import STATE_SUCCEEDED
import mbs_logging
from utils import yesterday_date, date_to_str

###############################################################################
# LOGGER
###############################################################################
logger = mbs_logging.logger

###############################################################################
##############################                 #################################
############################## Backup Auditors #################################
##############################                 #################################
###############################################################################

TYPE_PLAN_AUDIT = "PLAN_AUDIT"
TYPE_SINGLE_PLAN_AUDIT = "SINGLE_PLAN_AUDIT"

###############################################################################
# BackupAuditor
# Creates an audit report about backups taken as of a specific day.
#
class BackupAuditor(object):

    ###########################################################################
    def __init__(self):
        pass

    ###########################################################################
    def daily_audit_report(self, audit_date):
        pass

    ###########################################################################
    def yesterday_audit_reports_as_of(self):
        return self.daily_audit_report(yesterday_date())


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
        self._total_audits = 0
        self._total_success = 0
        self._total_failures = 0

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
    def has_failures(self):
        return self.failed_audits is not None and self.failed_audits

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
    def to_document(self):
        return {
            "_type": "AuditReport",
            "auditType": self.audit_type,
            "auditDate": self.audit_date,
            "failures": self._export_failures(),
            "totalAudits": self.total_audits,
            "totalSuccess": self.total_success,
            "totalFailures": self.total_failures,
            }

    ###########################################################################
    def _export_failures(self):
        return map(lambda entry: entry.to_document(),
                   self.failed_audits)


###############################################################################
# AuditEntry
###############################################################################

class AuditEntry(MBSObject):

    ###########################################################################
    def __init__(self):
        self._state = None
        self._backup = None

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
    def succeeded(self):
        return self.state == STATE_SUCCEEDED

    ###########################################################################
    @property
    def backup(self):
        return self._backup

    ###########################################################################
    @backup.setter
    def backup(self, backup):
        self._backup = backup

    ###########################################################################
    def to_document(self):
        doc =  {
            "state": self.state,
        }

        if self.backup:
            doc["backup"] = self.backup.to_document()

        return doc

###############################################################################
# PlanBackupAuditor
# Creates an audit report about backup plans taken yesterday.

class PlanAuditor(BackupAuditor):
    ###########################################################################
    def __init__(self,
                 plan_collection,
                 backup_collection):

        BackupAuditor.__init__(self)

        self._plan_collection = plan_collection
        self._backup_collection = backup_collection


    ###########################################################################
    # plan auditing
    ###########################################################################
    def daily_audit_report(self, audit_date):

        logger.info("PlanAuditor: Generating %s audit report "
                    "for '%s'" % (TYPE_PLAN_AUDIT, date_to_str(audit_date)))
        all_plans_report = AuditReport()
        all_plans_report.audit_date = audit_date
        all_plans_report.audit_type = TYPE_PLAN_AUDIT

        total_plans = 0
        failed_plan_reports = []
        for plan in self._plan_collection.find():
            plan_report = self._create_plan_audit_report(plan, audit_date)

            if plan_report.has_failures():
                failed_plan_reports.append(plan_report)

            total_plans += 1

        total_failures = len(failed_plan_reports)

        if failed_plan_reports:
            all_plans_report.failed_audits = failed_plan_reports

        all_plans_report.total_audits = total_plans
        all_plans_report.total_failures = total_failures
        all_plans_report.total_success = total_plans - total_failures

        logger.info("PlanAuditor: Generated report:\n%s " % all_plans_report)

        return all_plans_report

    ###########################################################################
    def _create_plan_audit_report(self, plan, audit_date):

        plan_report = PlanAuditReport()
        plan_report.plan = plan
        plan_report.audit_date = audit_date
        plan_report.audit_type = TYPE_SINGLE_PLAN_AUDIT

        failed_audits = []

        total_audits = 0

        for plan_occurrence in plan.natural_occurrences_as_of(audit_date):
            audit_entry = self._audit_plan_occurrence(plan, plan_occurrence)
            if audit_entry.failed():
                failed_audits.append(audit_entry)

            total_audits += 1

        total_failures = len(failed_audits)

        if failed_audits:
            plan_report.failed_audits = failed_audits

        plan_report.total_failures = total_failures
        plan_report.total_audits = total_audits
        plan_report.total_success = total_audits - total_failures

        return plan_report

    ###########################################################################
    def _audit_plan_occurrence(self, plan, plan_occurrence):
        backup = self._lookup_backup_by_plan_occurrence(plan,
                                                        plan_occurrence)

        audit_entry = PlanAuditEntry()

        if backup:
            audit_entry.backup = backup
            audit_entry.state = backup.state
        else:
            audit_entry.state = "NEVER SCHEDULED"


        audit_entry.plan_occurrence = plan_occurrence

        return audit_entry

    ###########################################################################
    def _lookup_backup_by_plan_occurrence(self, plan, plan_occurrence):

        q = {
            "plan._id": plan._id,
            "planOccurrence":plan_occurrence,
            }
        c = self._backup_collection

        return c.find_one(q)

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
    def to_document(self):
        doc = super(PlanAuditReport, self).to_document()
        doc["plan"] = self.plan.to_document()
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
    def to_document(self):
        doc = super(PlanAuditEntry, self).to_document()
        doc["planOccurrence"] = self.plan_occurrence

        return doc

###############################################################################
class GlobalAuditor():

    ###########################################################################
    def __init__(self, audit_collection, notification_handler=None):
        self._auditors = []
        self._audit_collection = audit_collection
        self._notification_handler = notification_handler

    ###########################################################################
    def register_auditor(self, auditor):
        self._auditors.append(auditor)

    ###########################################################################
    def generate_daily_audit_reports(self, date):
        reports = []
        for auditor in self._auditors:
            report = auditor.daily_audit_report(date)
            logger.info("GlobalAuditor: Saving audit report: \n%s" % report)
            self._audit_collection.save_document(report.to_document())
            reports.append(report)

        # send notification if specified
        if self._notification_handler:
            self._send_notification(date, reports)

    ###########################################################################
    def generate_yesterday_audit_reports(self):
        self.generate_daily_audit_reports(yesterday_date())

    ###########################################################################
    def _send_notification(self, date, reports):
        subject = "Backup Audit Reports for %s" % date_to_str(date)
        reports_str = map(str, reports)
        message = "\n\n\n".join(reports_str)
        self._notification_handler.send_notification(subject, message)