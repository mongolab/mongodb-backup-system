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

