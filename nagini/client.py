# -*- coding: utf8 -*-
import requests

from os.path import join, basename
import logging


logger = logging.getLogger(__name__)


class AzkabanClientError(Exception):
    pass


class AzkabanClient(object):
    session_id = None

    def __init__(self, host):
        self.host = host

    def login(self, username, password):
        r = requests.post(
            url=self.host,
            data={
                "action": "login",
                "username": username,
                "password": password
            }
        )

        if r.status_code != 200:
            raise AzkabanClientError('Server return error code: %d\n%s'
                                     % (r.status_code, r.text))
        try:
            json_data = r.json()
        except Exception as e:
            logger.exception('Error while parsing json on login.\n'
                             'Original response: %s', r.text)
            raise e

        if json_data.get('status') != "success":
            raise Exception("Authentication fails. Server return: %s" % json_data)
        else:
            self.session_id = json_data["session.id"]

    def _call_api(self, method, suffix, params, **kwargs):
        params.update({'session.id': self.session_id})
        r = requests.request(method, join(self.host, suffix), params=params, **kwargs)

        try:
            return r.json()
        except BaseException:
            params_ = params.copy()
            params_.pop('session.id')
            logger.exception('Error while api "%s" call "%s", params: %s', method, suffix, params)
            raise

    def delete_project(self, name):
        return self._call_api(method="get",
                              suffix="manager",
                              params={"delete": "true", "project": name})

    def create_project(self, name, description=""):
        return self._call_api(method="post",
                              suffix="manager",
                              params={"action": "create",
                                      "name": name,
                                      "description": description})

    def upload_project_zip(self, project, filename):
        r = self._call_api(
            method="post", suffix="manager", params={},
            data={"ajax": "upload", "project": project},
            files={
                "file": (basename(filename),
                         open(filename, "rb"),
                         "application/zip")
            }
        )
        err = 'None' if r is None else r.get('error', 'empty')
        if not r or r.get('error'):
            raise AzkabanClientError('Fail to upload project zip: %s' % err)
        return r

    def execute_flow(self, project, flow, properties=None, **kwargs):
        if properties:
            for key, value in properties.iteritems():
                kwargs["flowOverride[%s]" % key] = value
        kwargs.update({
            "ajax": "executeFlow",
            "project": project,
            "flow": flow
        })
        return self._call_api(method="get", suffix="executor", params=kwargs)

    def get_running_executions(self, project, flow):
        """Return list of ids of current running executions

        :param str project: name of project
        :param str flow: name of flow
        :rtype: list[int]
        """
        params = {
            "ajax": "getRunning",
            "project": project,
            "flow": flow
        }
        result = self._call_api("get", "executor", params)
        return result.get("execIds", [])

    def get_execution_info(self, exec_id):
        return self._call_api("get", "executor",
                              {"ajax": "fetchexecflow", "execid": exec_id})

    def get_job_logs(self, exec_id, job_name, offset=0, length=None):
        if length:
            return self._call_api("get", "executor", {
                "ajax": "fetchExecJobLogs",
                "execid": exec_id,
                "jobId": job_name,
                "offset": offset,
                "length": length
            })
        else:
            result = {"data": "", "length": None, "offset": offset}
            while True:
                ret = self._call_api("get", "executor", {
                    "ajax": "fetchExecJobLogs",
                    "execid": exec_id,
                    "jobId": job_name,
                    "offset": offset,
                    "length": 10000
                })
                result["data"] += ret["data"]
                if ret["length"] < 10000:
                    break
                offset += 10000
            result["length"] = len(result["data"])
            return result

    def reload_executors(self):
        return self._call_api('post', 'executor', {'ajax': 'reloadExecutors'})
