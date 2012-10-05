__author__ = 'abdul'


###############################################################################
##############################                #################################
############################## Backup Auditor #################################
##############################                #################################
###############################################################################

###############################################################################
# BackupAuditor
# Creates audit entries about backups taken yesterday. Those are of two types
# A- Auditing backups pertaining all account subscriptions
# B- Auditing backup plans
###############################################################################
TYPE_ACCOUNT_AUDIT = "ACCOUNT_AUDIT"
TYPE_PLAN_AUDIT = "PLAN_AUDIT"

class BackupAuditor(object):
    ###########################################################################
    def __init__(self, audit_collection,
                 account_collection,
                 plan_collection,
                 backup_collection):

        self._audit_collection = audit_collection
        self._accounts_collection = account_collection
        self._plan_collection = plan_collection
        self._backup_collection = backup_collection

    ###########################################################################
    def run(self):
        self._audit_account_backups()
        self._audit_plans()

    ###########################################################################
    # account auditing
    ###########################################################################
    def _audit_account_backups(self):
        for account in self._accounts_collection.find():
            subscriptions = account.get("subscriptions") or []
            for subscription in subscriptions:
                self._audit_subscription(account, subscription)

    ###########################################################################
    def _audit_subscription(self, account, subscription):
        resource_type = subscription["resource"]["_type"]
        resource_id = subscription["resource"]["id"]
        if resource_type == "db":
            self._audit_hosted_db(account, resource_id)

        elif resource_type == "server":
            self._audit_server(account, resource_id)

        elif resource_type == "cluster":
            self._audit_cluster(account, resource_id)

    ###########################################################################
    def _audit_hosted_db(self, account, hosted_db_id):
        cluster_id = get_hosted_db_cluster_id(hosted_db_id)
        backup_record = self._lookup_cluster_backup(cluster_id)
        resource = {"_type": "db",
                    "_id": hosted_db_id}

        self._create_audit_entry(audit_type=TYPE_ACCOUNT_AUDIT,
            backup_record=backup_record,
            account_id=account["_id"],
            resource=resource)

    ###########################################################################
    def _audit_server(self, account, server_id):
        backup_record = self._lookup_server_backup(server_id)
        resource = {"_type": "server",
                    "_id": server_id}
        self._create_audit_entry(audit_type=TYPE_ACCOUNT_AUDIT,
            backup_record=backup_record,
            account_id=account["_id"],
            resource=resource)

    ###########################################################################
    def _audit_cluster(self, account, cluster_id):
        backup_record = self._lookup_cluster_backup(cluster_id)
        resource = {"_type": "cluster",
                    "_id": cluster_id}
        self._create_audit_entry(audit_type=TYPE_ACCOUNT_AUDIT,
            backup_record=backup_record,
            account_id=account["_id"],
            resource=resource)

    ###########################################################################
    def _lookup_server_backup(self, server_id):

        source= {
            "_type": "backup.backup_new.MongoLabServerSource",
            "serverId": server_id,
            }

        return self._lookup_backup_by_source(source)

    ###########################################################################
    def _lookup_cluster_backup(self, cluster_id):
        source= {
            "_type": "backup.backup_new.MongoLabClusterSource",
            "clusterId": cluster_id,
            }

        return self._lookup_backup_by_source(source)

    ###########################################################################
    def _lookup_backup_by_source(self, source):
        date_start = yesterday_date()
        date_end = today_date()

        q = {
            "state": STATE_SUCCEEDED,
            "timestamp":{"$gte": date_start, "$lt": date_end},
            "source": source
        }

        c = self._backup_collection
        results = c.find(q).sort("timestamp", -1).limit(1)

        if results is not None and results.count(True) > 0:
            return new_backup(results[0])

    ###########################################################################
    def _lookup_backup_by_plan_occurrence(self, plan, plan_occurrence):

        q = {
            "state": STATE_SUCCEEDED,
            "plan._id": plan._id,
            "planOccurrence":plan_occurrence,
            }
        c = self._backup_collection

        return c.find_one(q)

    ###########################################################################
    def _create_audit_entry(self, audit_type, backup_record,
                            account_id=None, resource=None,
                            plan=None, plan_occurrence=None):

        yesterday = yesterday_date()
        audit_date = (plan_occurrence if audit_type == TYPE_PLAN_AUDIT
                      else yesterday)

        audit_entry = AuditEntry()
        audit_entry.audited_date = audit_date
        audit_entry.audit_type = audit_type
        audit_entry.backup_record = backup_record
        audit_entry.is_backed_up = backup_record is not None
        audit_entry.account_id = account_id
        audit_entry.resource = resource
        audit_entry.plan = plan
        audit_entry.plan_occurrence = plan_occurrence
        self._audit_collection.save(audit_entry._entry_document)

    ###########################################################################
    # plan auditing
    ###########################################################################
    def _audit_plans(self):
        for plan_doc in self._plan_collection.find():
            plan = new_plan(plan_doc)
            self._audit_plan(plan)

    ###########################################################################
    def _audit_plan(self, plan):
        for plan_occurrence in plan.natural_occurrences_yesterday():
            self._audit_plan_occurrence(plan, plan_occurrence)

    ###########################################################################
    def _audit_plan_occurrence(self, plan, plan_occurrence):
        backup_record = self._lookup_backup_by_plan_occurrence(plan,
            plan_occurrence)
        self._create_audit_entry(audit_type=TYPE_PLAN_AUDIT,
            plan=plan,
            plan_occurrence=plan_occurrence,
            backup_record=backup_record)

###############################################################################
# AuditEntry
###############################################################################
class AuditEntry(object):
    def __init__(self, entry_doc=None):
        self._entry_document = entry_doc or {}

    ###########################################################################
    @property
    def _id(self):
        return str(self._entry_document['_id'])

    ###########################################################################
    @property
    def audited_date(self):
        return self._entry_document['auditedDate']


    @audited_date.setter
    def audited_date(self, value):
        self._entry_document['auditedDate'] = value

    ###########################################################################
    @property
    def audit_type(self):
        return self._entry_document['auditType']


    @audit_type.setter
    def audit_type(self, value):
        self._entry_document['auditType'] = value

    ###########################################################################
    @property
    def account_id(self):
        return self._entry_document['accountId']

    @account_id.setter
    def account_id(self, value):
        self._entry_document['accountId'] = value

    ###########################################################################
    @property
    def resource(self):
        return self._entry_document['resource']

    @resource.setter
    def resource(self, value):
        self._entry_document['resource'] = value

    ###########################################################################
    @property
    def is_backed_up(self):
        return self._entry_document['isBackedup']

    @is_backed_up.setter
    def is_backed_up(self, value):
        self._entry_document['isBackedup'] = value

    ###########################################################################
    @property
    def backup_record(self):
        return self._entry_document['backupRecord']

    ###########################################################################
    @backup_record.setter
    def backup_record(self, backup):
        if backup:
            self._entry_document['backupRecord'] = backup._backup_document

    ###########################################################################
    @property
    def plan(self):
        return self._entry_document['plan']

    @plan.setter
    def plan(self, plan):
        if plan:
            self._entry_document['plan'] = plan.plan_document

    ###########################################################################
    @property
    def plan_occurrence(self):
        return self._entry_document['planOccurrence']

    @plan_occurrence.setter
    def plan_occurrence(self, value):
        self._entry_document['planOccurrence'] = value
