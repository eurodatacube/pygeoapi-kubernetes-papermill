# =================================================================
#
# Authors: Bernhard Mallinger <bernhard.mallinger@eox.at>
#
# Copyright (C) 2024 EOX IT Services GmbH <https://eox.at>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from __future__ import annotations

import logging
from typing import Optional, Any

from kubernetes import client as k8s_client, config as k8s_config

from pygeoapi.process.manager.base import BaseManager
from pygeoapi.util import (
    JobStatus,
    Subscriber,
    RequestedResponse,
)

# TODO: move elsewhere if we keep this
from .kubernetes import JobDict


from .common import current_namespace, k8s_job_name

LOGGER = logging.getLogger(__name__)


class ArgoManager(BaseManager):
    def __init__(self, manager_def: dict) -> None:
        super().__init__(manager_def)

        self.is_async = True
        self.supports_subscribing = True

        if manager_def.get("skip_k8s_setup"):
            # this is virtually only useful for tests
            self.namespace = "test"
        else:
            try:
                k8s_config.load_kube_config()
            except Exception:
                # load_kube_config might throw anything :/
                k8s_config.load_incluster_config()

            self.namespace = current_namespace()

        self.workflow_template: str = manager_def["workflow_template"]
        self.custom_objects_api = k8s_client.CustomObjectsApi()
        # self.core_api = k8s_client.CoreV1Api()

    def get_jobs(self, status=None, limit=None, offset=None) -> dict:
        """
        Get process jobs, optionally filtered by status

        :param status: job status (accepted, running, successful,
                       failed, results) (default is all)
        :param limit: number of jobs to return
        :param offset: pagination offset

        :returns: dict of list of jobs (identifier, status, process identifier)
                  and numberMatched
        """
        raise NotImplementedError

    def get_job(self, job_id) -> Optional[JobDict]:
        """
        Returns the actual output from a completed process

        :param job_id: job identifier

        :returns: `dict`  # `pygeoapi.process.manager.Job`
        """
        raise NotImplementedError

    def add_job(self, job_metadata):
        """
        Add a job

        :param job_metadata: `dict` of job metadata

        :returns: add job result
        """
        # For k8s, add_job is implied by executing the job
        return

    def update_job(self, processid, job_id, update_dict):
        """
        Updates a job

        :param processid: process identifier
        :param job_id: job identifier
        :param update_dict: `dict` of property updates

        :returns: `bool` of status result
        """
        # we could update the metadata by changing the job annotations
        raise NotImplementedError("Currently there's no use case for updating k8s jobs")

    def get_job_result(self, job_id) -> tuple[Optional[Any], Optional[str]]:
        """
        Returns the actual output from a completed process

        :param job_id: job identifier

        :returns: `tuple` of mimetype and raw output
        """

        # TODO: fetch from argo somehow
        raise NotImplementedError

    def delete_job(self, job_id) -> bool:
        """
        Deletes a job

        :param processid: process identifier
        :param job_id: job identifier

        :returns: `bool` of status result
        """
        raise NotImplementedError

    def _execute_handler_async(
        self,
        p: ArgoManager,
        job_id,
        data_dict,
        requested_outputs: Optional[dict] = None,
        subscriber: Optional[Subscriber] = None,
        requested_response: Optional[RequestedResponse] = RequestedResponse.raw.value,  # noqa
    ) -> tuple[str, dict, JobStatus]:
        """
        In practise k8s jobs are always async.

        :param p: `pygeoapi.process` object
        :param job_id: job identifier
        :param data_dict: `dict` of data parameters

        :returns: tuple of None (i.e. initial response payload)
                  and JobStatus.accepted (i.e. initial job status)
        """

        api_group = "argoproj.io"
        api_version = "v1alpha1"

        # TODO test with this
        # https://github.com/argoproj/argo-workflows/blob/main/examples/workflow-template/workflow-template-ref-with-entrypoint-arg-passing.yaml
        body = {
            "apiVersion": f"{api_group}/{api_version}",
            "kind": "Workflow",
            "metadata": {
                "name": k8s_job_name(job_id),
                "namespace": self.namespace,
                # TODO: labels to identify our jobs?
                # "labels": {}
            },
            "spec": {
                "arguments": {
                    "parameters": [
                        {"name": key, "value": value}
                        for key, value in data_dict.items()
                    ]
                },
                "entrypoint": "test",
                "workflowTemplateRef": {"name": self.workflow_template},
            },
        }
        self.custom_objects_api.create_namespaced_custom_object(
            group=api_group,
            version=api_version,
            namespace=self.namespace,
            plural="workflows",
            body=body,
        )
        return ("application/json", {}, JobStatus.accepted)
