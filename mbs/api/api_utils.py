__author__ = 'abdul'

import traceback

from mbs.utils import document_pretty_string, parse_json
from mbs.errors import MBSApiError
from flask.globals import request

from mbs.mbs import get_mbs
from bson.objectid import ObjectId

########################################################################################################################
# HELPERS
########################################################################################################################
def error_response(message, **kwargs):
    kwargs.update({"error": message})
    return document_pretty_string(kwargs)

########################################################################################################################
def ok_response(ok=True):
    return document_pretty_string({
        "ok": ok
    })

########################################################################################################################
def get_request_json():
    if request.data:
        return parse_json(request.data)

########################################################################################################################
def raise_service_unvailable():
    raise MBSApiError("Service Unavailable", status_code=503)

########################################################################################################################
def send_api_error(end_point, exception):
    subject = "BackupSystemAPI Error"
    message = ("BackupSystemAPI Error on '%s'.\n\nStack Trace:\n%s" %
               (end_point, traceback.format_exc()))

    get_mbs().notifications.send_error_notification(subject, message)

########################################################################################################################
def raise_forbidden_error(msg):
    raise MBSApiError(msg, status_code=403)

########################################################################################################################
def new_request_id():
    return str(ObjectId())


########################################################################################################################
def get_request_value(key):
    if request.data:
        if request.json.get(key):
            return request.json.get(key)

    if request.args and request.args.get(key):
        return request.args.get(key)
