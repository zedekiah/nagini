# -*- coding: utf8 -*-
from os.path import join, basename
import requests


class AzkabanClient(object):
    session_id = None

    def __init__(self, host):
        self.host = host

    def login(self, username, password):
        r = requests.post(
            url=self.host,
            params={
                "action": "login",
                "username": username,
                "password": password
            }
        )

        json_data = r.json()

        if json_data["status"] != "success":
            raise Exception("Authentication fails")
        else:
            self.session_id = json_data["session.id"]

    def _call_api(self, method, suffix, params, **kwargs):
        params.update({"session.id": self.session_id})
        r = requests.request(
            method, join(self.host, suffix), params=params, **kwargs
        )
        try:
            return r.json()
        except ValueError:
            return None

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
        return self._call_api(
            method="post", suffix="manager", params={},
            data={"ajax": "upload", "project": project},
            files={
                "file": (basename(filename),
                         open(filename, "rb"),
                         "application/zip")
            }
        )

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
