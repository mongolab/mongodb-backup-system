__author__ = 'abdul'

import traceback

from mbs import get_mbs
from audit import *
from globals import State
import logging
from date_utils import yesterday_date, datetime_to_string, date_plus_seconds

from mbs.notification import NotificationPriority

###############################################################################
# LOGGER
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

###############################################################################
##############################                 ################################
############################## Backup Auditors ################################
##############################                 ################################
###############################################################################

TYPE_PLAN_AUDIT = "PLAN_AUDIT"
TYPE_SINGLE_PLAN_AUDIT = "SINGLE_PLAN_AUDIT"
TYPE_PLAN_RETENTION_AUDIT = "PLAN_RETENTION_AUDIT"

###############################################################################
# BackupAuditor
# Creates an audit report about backups taken as of a specific day.
#
class BackupAuditor(object):

    ###########################################################################
    def __init__(self):
        self._max_allowed_failures_percentage = 0

    ###########################################################################
    def audit_report(self, audit_date):
        pass

    ###########################################################################
    def daily_audit_report(self, audit_date):
        pass

    ###########################################################################
    def yesterday_audit_reports_as_of(self):
        return self.daily_audit_report(yesterday_date())

    ###########################################################################
    @property
    def name(self):
        return "BACKUP AUDITOR"

    @property
    def max_allowed_failures_percentage(self):
        return self._max_allowed_failures_percentage

    @max_allowed_failures_percentage.setter
    def max_allowed_failures_percentage(self, v):
        self._max_allowed_failures_percentage = v

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
            self.generate_audit_report(auditor, date)

    ###########################################################################
    def generate_audit_report(self, auditor, date):
        try:
            report = auditor.daily_audit_report(date)
            logger.info("GlobalAuditor: Saving audit report: \n%s" % report)
            self._audit_collection.save_document(report.to_document())

            # send audit report
            self._send_audit_report(auditor, report)
        except Exception, e:
            sbj = "Auditor %s Error" % auditor.name
            msg = ("Auditor %s Error!.\n\nStack Trace:\n%s" %
                   (auditor.name, traceback.format_exc()))
            get_mbs().notifications.send_error_notification(sbj, msg)

    ###########################################################################
    def generate_yesterday_audit_reports(self):
        self.generate_daily_audit_reports(yesterday_date())

    ###########################################################################
    def _send_audit_report(self, auditor, report):
        subject = ("%s Audit Report for %s" %
                   (auditor.name, datetime_to_string(report.audit_date)))

        message = report.summary()
        get_mbs().notifications.send_notification(subject, message,
                                                  notification_type="audit")

###############################################################################
# PlanScheduleAuditor
# Creates an audit report about backup plans taken yesterday.

class PlanScheduleAuditor(BackupAuditor):
    ###########################################################################
    def __init__(self):
        BackupAuditor.__init__(self)

    ###########################################################################
    # plan auditing
    ###########################################################################
    def daily_audit_report(self, audit_date):

        logger.info("PlanScheduleAuditor: Generating %s audit report for '%s'"
                    % (TYPE_PLAN_AUDIT,  datetime_to_string(audit_date)))

        audit_end_date = date_plus_seconds(audit_date, 3600 * 24)
        all_plans_report = PlanScheduleAuditReport()
        all_plans_report.audit_date = audit_date
        all_plans_report.audit_type = TYPE_PLAN_AUDIT

        total_plans = 0
        failed_plan_reports = []
        all_warned_audits = []
        total_warnings = 0
        for plan in get_mbs().plan_collection.find_iter(no_cursor_timeout=True):
            plan_report = self._create_plan_audit_report(plan, audit_date)

            if plan_report.has_failures():
                failed_plan_reports.append(plan_report)
            if plan_report.has_warnings():
                # only append to warned audits if report doesn't have failures
                if not plan_report.has_failures():
                    all_warned_audits.extend(plan_report.warned_audits)

                total_warnings += 1

            total_plans += 1

        total_failures = len(failed_plan_reports)

        if failed_plan_reports:
            all_plans_report.failed_audits = failed_plan_reports
        if all_warned_audits:
            all_plans_report.warned_audits = all_warned_audits

        all_plans_report.total_audits = total_plans
        all_plans_report.total_failures = total_failures
        all_plans_report.total_success = total_plans - total_failures
        all_plans_report.total_warnings = total_warnings

        logger.info("PlanScheduleAuditor: Generated report:\n%s " %
                    all_plans_report)


        # alert if failed audits are >= max allowed percent of total
        if (not self.max_allowed_failures_percentage or
                (total_failures * self.max_allowed_failures_percentage * 100 >= total_plans)):
            subject = "%s Auditor Failure: Too many failures!!!" % self.name
            msg = "There are %s failures out of %s which is >= 1%%" % (total_failures, total_plans)
            logger.error(subject)
            logger.error(msg)
            get_mbs().notifications.send_event_notification(subject, msg, priority=NotificationPriority.CRITICAL)

        return all_plans_report

    ###########################################################################
    def _create_plan_audit_report(self, plan, audit_date):

        plan_report = PlanAuditReport()
        plan_report.plan = plan
        plan_report.audit_date = audit_date
        plan_report.audit_type = TYPE_SINGLE_PLAN_AUDIT

        failed_audits = []
        warned_audits = []
        total_audits = 0
        total_warnings = 0
        for plan_occurrence in plan.schedule.natural_occurrences_as_of(
                audit_date):
            # skip occurrences before plan's created date
            if plan.created_date and plan_occurrence < plan.created_date:
                continue

            audit_entry = self._audit_plan_occurrence(plan, plan_occurrence)
            if audit_entry.failed():
                failed_audits.append(audit_entry)

            if audit_entry.warned():
                # only append to warned audits if audit entry succeeded
                if audit_entry.succeeded():
                    warned_audits.append(audit_entry)
                total_warnings += 1

            total_audits += 1

        total_failures = len(failed_audits)

        if failed_audits:
            plan_report.failed_audits = failed_audits

        if warned_audits:
            plan_report.warned_audits = warned_audits

        plan_report.total_failures = total_failures
        plan_report.total_audits = total_audits
        plan_report.total_success = total_audits - total_failures
        plan_report.warned_audits = warned_audits
        plan_report.total_warnings = total_warnings

        return plan_report

    ###########################################################################
    def _audit_plan_occurrence(self, plan, plan_occurrence):
        backup = _lookup_backup_by_plan_occurrence(plan, plan_occurrence)

        audit_entry = PlanAuditEntry()

        if backup:
            audit_entry.backup_id = backup.id
            audit_entry.state = backup.state
            audit_entry.errors = backup.get_error_logs()
            audit_entry.warnings = backup.get_warning_logs()
        else:
            audit_entry.state = "NEVER SCHEDULED"


        audit_entry.plan_occurrence = plan_occurrence

        return audit_entry

    ###########################################################################
    @property
    def name(self):
        return "PlanScheduleAuditor"

###############################################################################
def _lookup_backup_by_plan_occurrence(plan, plan_occurrence):

    q = {
        "plan._id": plan._id,
        "planOccurrence":plan_occurrence,
        }
    c = get_mbs().backup_collection

    return c.find_one(q)

##############################################################################
# PlanRetentionAuditor
# Creates an audit report about plan retention


class PlanRetentionAuditor(BackupAuditor):
    ###########################################################################
    def __init__(self):
        BackupAuditor.__init__(self)

    ###########################################################################
    # plan auditing
    ###########################################################################

    ###########################################################################
    def daily_audit_report(self, audit_date):
        return self.audit_report(audit_date)

    ###########################################################################
    def audit_report(self, audit_date):

        logger.info("PlanRetentionAuditor: Generating %s audit report for "
                    "'%s'" % (TYPE_PLAN_RETENTION_AUDIT,
                              datetime_to_string(audit_date)))

        all_plans_report = PlanScheduleAuditReport()
        all_plans_report.audit_date = audit_date
        all_plans_report.audit_type = TYPE_PLAN_RETENTION_AUDIT

        total_plans = 0
        failed_plan_reports = []
        all_warned_audits = []
        total_warnings = 0

        for plan in get_mbs().plan_collection.find_iter(no_cursor_timeout=True):
            logger.info("=== Processing Plan '%s'..." % plan.id)
            if not plan.retention_policy:
                logger.warning("Plan '%s' has not retention policy! "
                               "Skipping..." % plan.id)
                continue

            plan_report = self._create_plan_audit_report(plan, audit_date)

            if plan_report.has_failures():
                failed_plan_reports.append(plan_report)
            if plan_report.has_warnings():
                # only append to warned audits if report doesn't have failures
                if not plan_report.has_failures():
                    all_warned_audits.extend(plan_report.warned_audits)

                total_warnings += 1

            total_plans += 1

        total_failures = len(failed_plan_reports)

        if failed_plan_reports:
            all_plans_report.failed_audits = failed_plan_reports
        if all_warned_audits:
            all_plans_report.warned_audits = all_warned_audits

        all_plans_report.total_audits = total_plans
        all_plans_report.total_failures = total_failures
        all_plans_report.total_success = total_plans - total_failures
        all_plans_report.total_warnings = total_warnings

        logger.info("PlanRetentionAuditor: Generated report:\n%s " %
                    all_plans_report)

        return all_plans_report

    ###########################################################################
    def _create_plan_audit_report(self, plan, audit_date):

        plan_report = PlanAuditReport()
        plan_report.plan = plan
        plan_report.audit_date = audit_date
        plan_report.audit_type = TYPE_SINGLE_PLAN_AUDIT

        failed_audits = []
        warned_audits = []
        total_audits = 0
        total_warnings = 0
        rp = plan.retention_policy
        logger.info("Finding all occurrences for plan '%s' be retained as "
                    "of %s" % (plan.id, audit_date))

        for occurrence in rp.get_plan_occurrences_to_retain_as_of(plan,
                                                                  audit_date):
            logger.info("Auditing occurrence '%s' " % occurrence)
            # skip occurrences before plan's created date
            if plan.created_date and occurrence < plan.created_date:
                logger.info("Skipping occurrence '%s' because its before plan "
                            "created date '%s'" %
                            (occurrence, plan.created_date))
                continue

            backup = _lookup_backup_by_plan_occurrence(plan, occurrence)
            if not backup or backup.state != State.SUCCEEDED:
                logger.info("Skipping occurrence '%s' because its backup did"
                            " not get scheduled or did not succeed. Should be "
                            "caught by the PlanScheduleAuditor." % occurrence)
                continue

            audit_entry = self._audit_occurrence_retention(occurrence, backup,
                                                           audit_date)

            if audit_entry.failed():
                failed_audits.append(audit_entry)

            if audit_entry.warned():
                # only append to warned audits if audit entry succeeded
                if audit_entry.succeeded():
                    warned_audits.append(audit_entry)
                total_warnings += 1

            total_audits += 1

        total_failures = len(failed_audits)

        if failed_audits:
            plan_report.failed_audits = failed_audits

        if warned_audits:
            plan_report.warned_audits = warned_audits

        plan_report.total_failures = total_failures
        plan_report.total_audits = total_audits
        plan_report.total_success = total_audits - total_failures
        plan_report.warned_audits = warned_audits
        plan_report.total_warnings = total_warnings

        return plan_report

    ###########################################################################
    def _audit_occurrence_retention(self, plan_occurrence, backup, audit_date):

        audit_entry = PlanAuditEntry()

        # check if the backup expired before it was supposed to be expired
        if backup.expired_date and backup.expired_date < audit_date:
            audit_entry.state = "BACKUP EXPIRED AND NOT RETAINED"
        else:
            audit_entry.state = State.SUCCEEDED

        audit_entry.plan_occurrence = plan_occurrence

        return audit_entry

    ###########################################################################
    @property
    def name(self):
        return "PlanRetentionAuditor"
