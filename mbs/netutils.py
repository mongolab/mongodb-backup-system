import httplib2
import json

###############################################################################
# curl functionality
###############################################################################

def get_url_json(url, headers=None, timeout=None, retries=0):
    return fetch_url_json(url, method="GET", headers=headers, timeout=timeout, retries=retries)

def post_url_json(url, data, headers=None, timeout=None, retries=0):
    return fetch_url_json(url, method="POST", data=data, headers=headers, timeout=timeout, retries=retries)

def fetch_url_json(url, method=None, data=None, headers=None, timeout=None, retries=None):
    if not headers:
        headers = {}
    headers["Content-Type"] = "application/json"
    if data and isinstance(data, dict):
        data = json.dumps(data)
    result = fetch_url(url, method=method, data=data, headers=headers, timeout=timeout, retries=retries)
    if result and not isinstance(result, bool):
        return json.loads(result)
    else:
        return result

def fetch_url(url, method=None, data=None, headers=None, timeout=None, retries=None):
    http = httplib2.Http(timeout=timeout)
    retries = retries or 0
    _response = None
    _content = None
    if data and not isinstance(data, str):
        data = str(data)
    while retries >= 0 and (_response is None or _response["status"] != "200"):
        try:
            retries -= 1
            _response, _content = http.request(url, method=method or "GET", body=data, headers=headers)
        except Exception,e:
            if retries < 0:
                raise
    if _response is None or "status" not in _response:
        raise Exception("Error: Response is empty: %s" % _content)
    if _response["status"] != "200":
        raise Exception("Error (%s): %s" % (_response["status"], _content))
    if _content:
        return _content
    else:
        return True


###############################################################################
# Flask cross domain decorator
###############################################################################
# http://flask.pocoo.org/snippets/56/

from datetime import timedelta
from flask import make_response, request, current_app
from functools import update_wrapper


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

