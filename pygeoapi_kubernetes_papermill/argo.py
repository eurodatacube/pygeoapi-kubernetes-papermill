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

import datetime
import logging
from typing import Optional, Any, cast

from kubernetes import client as k8s_client, config as k8s_config

from http import HTTPStatus
import json

import kubernetes.client.rest


from pygeoapi.process.manager.base import BaseManager, DATETIME_FORMAT, BaseProcessor
from pygeoapi.util import (
    JobStatus,
    Subscriber,
    RequestedResponse,
    ProcessExecutionMode,
)

from pygeoapi.process.base import (
    JobNotFoundError,
)

from .common import (
    k8s_job_name,
    current_namespace,
    format_annotation_key,
    now_str,
    parse_annotation_key,
    hide_secret_values,
    JobDict,
)


LOGGER = logging.getLogger(__name__)

WORKFLOWS_API_GROUP = "argoproj.io"
WORKFLOWS_API_VERSION = "v1alpha1"

K8S_CUSTOM_OBJECT_WORKFLOWS = {
    "group": WORKFLOWS_API_GROUP,
    "version": WORKFLOWS_API_VERSION,
    "plural": "workflows",
}


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
        try:
            k8s_wf: dict = self.custom_objects_api.get_namespaced_custom_object(
                **K8S_CUSTOM_OBJECT_WORKFLOWS,
                name=k8s_job_name(job_id=job_id),
                namespace=self.namespace,
            )
            return job_from_k8s_wf(k8s_wf)
        except kubernetes.client.rest.ApiException as e:
            if e.status == HTTPStatus.NOT_FOUND:
                raise JobNotFoundError
            else:
                raise

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
        p: ArgoProcessor,
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

        annotations = {
            "identifier": job_id,
            "process_id": p.metadata.get("id"),
            "job_start_datetime": now_str(),
        }

        body = {
            "apiVersion": f"{WORKFLOWS_API_GROUP}/{WORKFLOWS_API_VERSION}",
            "kind": "Workflow",
            "metadata": {
                "name": k8s_job_name(job_id),
                "namespace": self.namespace,
                # TODO: labels to identify our jobs?
                # "labels": {}
                "annotations": {
                    format_annotation_key(k): v for k, v in annotations.items()
                },
            },
            "spec": {
                "arguments": {
                    "parameters": [
                        {"name": key, "value": value}
                        for key, value in data_dict.items()
                    ]
                },
                "entrypoint": "test",
                "workflowTemplateRef": {"name": p.workflow_template},
            },
        }
        self.custom_objects_api.create_namespaced_custom_object(
            **K8S_CUSTOM_OBJECT_WORKFLOWS,
            namespace=self.namespace,
            body=body,
        )
        return ("application/json", {}, JobStatus.accepted)


class ArgoProcessor(BaseProcessor):
    def __init__(self, processor_def: dict) -> None:
        metadata = {
            "version": "0.1.0",
            "id": "",
            "title": "",
            "description": "",
            "keywords": [""],
            "links": [],
            "inputs": {},
            "outputs": {},
            "example": {},
            "jobControlOptions": [
                ProcessExecutionMode.async_execute.value,
                ProcessExecutionMode.sync_execute.value,
            ],
        }
        super().__init__(processor_def, metadata)

        self.workflow_template: str = processor_def["workflow_template"]


def job_from_k8s_wf(workflow: dict) -> JobDict:
    annotations = workflow["metadata"]["annotations"] or {}
    metadata = {
        parsed_key: v
        for orig_key, v in annotations.items()
        if (parsed_key := parse_annotation_key(orig_key))
    }

    metadata["parameters"] = json.dumps(
        hide_secret_values(
            {
                param["name"]: param["value"]
                for param in workflow["spec"]["arguments"]["parameters"]
            }
        )
    )

    status = status_from_argo_phase(workflow["status"]["phase"])

    if started_at := workflow["status"].get("startedAt"):
        metadata["job_start_datetime"] = argo_date_str_to_pygeoapi_date_str(started_at)
    if finished_at := workflow["status"].get("finishedAt"):
        metadata["job_end_datetime"] = argo_date_str_to_pygeoapi_date_str(finished_at)
    default_progress = "100" if status == JobStatus.successful else "1"
    # TODO: parse progress fromm wf status progress "1/2"

    return cast(
        JobDict,
        {
            # need this key in order not to crash, overridden by metadata:
            "identifier": "",
            "process_id": "",
            "job_start_datetime": "",
            "status": status.value,
            "mimetype": None,  # we don't know this in general
            "message": "",  # TODO: what to show here?
            "progress": default_progress,
            **metadata,
        },
    )


def argo_date_str_to_pygeoapi_date_str(argo_date_str: str) -> str:
    ARGO_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    return datetime.datetime.strptime(
        argo_date_str,
        ARGO_DATE_FORMAT,
    ).strftime(DATETIME_FORMAT)


def status_from_argo_phase(phase: str) -> JobStatus:
    if phase == "Pending":
        return JobStatus.accepted
    elif phase == "Running":
        return JobStatus.running
    elif phase == "Succeeded":
        return JobStatus.successful
    elif phase == "Failed":
        return JobStatus.failed
    elif phase == "Error":
        return JobStatus.failed
    elif phase == "":
        return JobStatus.accepted
    else:
        raise AssertionError(f"Invalid argo wf phase {phase}")
