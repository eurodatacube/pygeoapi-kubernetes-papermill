# =================================================================
#
# Authors: Bernhard Mallinger <bernhard.mallinger@eox.at>
#
# Copyright (C) 2020 EOX IT Services GmbH <https://eox.at>
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

from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
import json
import logging
import re
import time
from threading import Thread
from typing import Literal, Optional, Any, TypedDict, cast
import os

from kubernetes import client as k8s_client, config as k8s_config
import kubernetes.client.rest
import requests

from pygeoapi.util import (
    JobStatus,
    Subscriber,
    RequestedResponse,
)
from pygeoapi.process.base import (
    BaseProcessor,
    JobNotFoundError,
    JobResultNotFoundError,
)
from pygeoapi.process.manager.base import BaseManager, DATETIME_FORMAT

from .common import is_k8s_job_name, k8s_job_name

LOGGER = logging.getLogger(__name__)


class KubernetesProcessor(BaseProcessor):
    @dataclass(frozen=True)
    class JobPodSpec:
        pod_spec: k8s_client.V1PodSpec
        extra_annotations: dict[str, str]
        extra_labels: dict[str, str]

    def create_job_pod_spec(
        self,
        data: dict,
        job_name: str,
    ) -> JobPodSpec:
        """
        Returns a definition of a job as well as result handling.
        Currently the only supported way for handling result is for the processor
        to provide a fixed link where the results will be available (the job itself
        has to ensure that the resulting data ends up at the link)
        """
        raise NotImplementedError()

    def execute(self):
        raise NotImplementedError(
            "Kubernetes Processes can't be executed directly, use KubernetesManager"
        )


JobDict = TypedDict(
    "JobDict",
    {
        "identifier": str,
        "status": str,
        "result-notebook": str,
        "message": str,
        "job_end_datetime": Optional[str],
    },
    total=False,
)


class KubernetesManager(BaseManager):
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

            # NOTE: this starts a thread per WSGI_WORKER, which is not optimal
            # the eoxhub use case uses only 1 worker, so it's trivially fine.
            # not sure how this can be solved cleanly on different web servers.
            Thread(
                group=None,
                target=job_babysitter,
                daemon=True,
                name="JobBabysitter",
                kwargs={"namespace": self.namespace},
            ).start()

        self.batch_v1 = k8s_client.BatchV1Api()
        self.core_api = k8s_client.CoreV1Api()

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

        def get_start_time_from_job(job: k8s_client.V1Job) -> str:
            key = format_annotation_key("job_start_datetime")
            return job.metadata.annotations.get(key, "")

        # NOTE: pagination should be pushed to the kubernetes api,
        #       but it doesn't support regex matching on the job name
        #       https://github.com/kubernetes-client/python/issues/171#issuecomment-428077215
        k8s_jobs = sorted(
            (
                k8s_job
                for k8s_job in self.batch_v1.list_namespaced_job(
                    namespace=self.namespace,
                ).items
                if is_k8s_job_name(k8s_job.metadata.name)
            ),
            key=get_start_time_from_job,
            reverse=True,
        )

        number_matched = len(k8s_jobs)

        # NOTE: need to paginate before expensive single job serialization
        if offset:
            k8s_jobs = k8s_jobs[offset:]

        if limit:
            k8s_jobs = k8s_jobs[:limit]

        # TODO: implement status filter

        return {
            "jobs": [
                job_from_k8s(k8s_job, self._job_message(k8s_job))
                for k8s_job in k8s_jobs
            ],
            "numberMatched": number_matched,
        }

    def get_job(self, job_id) -> Optional[JobDict]:
        """
        Returns the actual output from a completed process

        :param job_id: job identifier

        :returns: `dict`  # `pygeoapi.process.manager.Job`
        """

        try:
            k8s_job: k8s_client.V1Job = self.batch_v1.read_namespaced_job(
                name=k8s_job_name(job_id=job_id),
                namespace=self.namespace,
            )
            return job_from_k8s(k8s_job, self._job_message(k8s_job))
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
        # NOTE: it's a breach of abstraction to use notebook-related code here,
        #       but it's useful now and complicated approach doesn't seem warrented
        # avoid import loop
        from .notebook import notebook_job_output

        job = self.get_job(job_id=job_id)

        if job is None or (JobStatus[job["status"]]) != JobStatus.successful:
            raise JobResultNotFoundError
        else:
            return notebook_job_output(job)

    def delete_job(self, job_id) -> bool:
        """
        Deletes a job

        :param processid: process identifier
        :param job_id: job identifier

        :returns: `bool` of status result
        """
        LOGGER.debug(f"Deleting job {job_id}")

        job_name = k8s_job_name(job_id=job_id)

        try:
            job: k8s_client.V1Job = self.batch_v1.read_namespaced_job(
                name=job_name,
                namespace=self.namespace,
            )
        except kubernetes.client.rest.ApiException as e:
            if e.status == HTTPStatus.NOT_FOUND:
                return False
            else:
                raise

        pod = self._pod_for_job(job)

        LOGGER.info(f"Delete job {job_name}")
        self.batch_v1.delete_namespaced_job(
            name=job_name,
            namespace=self.namespace,
            # this policy should also remove pods, but doesn't
            propagation_policy="Foreground",
        )

        job_dict = job_from_k8s(job, message=None)
        LOGGER.debug(f"Deleting file {job_dict['result-notebook']}")
        # NOTE: this assumes that we have user home under the same path as jupyter
        os.remove(job_dict["result-notebook"])

        # it should be possible for k8s to delete pods when deleting jobs,
        # but it doesn't appear that it's working reliably, so reimplement it here.
        # https://github.com/kubernetes/kubernetes/issues/20902
        if pod:
            LOGGER.info(f"Delete pod {pod.metadata.name}")
            self.core_api.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=self.namespace,
                # NOTE: this is equivalent to force delete. we have to use it containers
                # can get stuck due to k8s not cleaning up, which prevents autoscaling
                # from removing nodes and thus incurring costs.
                grace_period_seconds=0,
            )
        return True

    def _execute_handler_sync(
        self,
        p: BaseProcessor,
        job_id,
        data_dict: dict,
        requested_outputs: Optional[dict] = None,
        subscriber: Optional[Subscriber] = None,
        requested_response: Optional[RequestedResponse] = RequestedResponse.raw.value,  # noqa
    ) -> tuple[Optional[str], Optional[Any], JobStatus]:
        """
        Synchronous execution handler

        :param p: `pygeoapi.t` object
        :param job_id: job identifier
        :param data_dict: `dict` of data parameters

        :returns: tuple of MIME type, response payload and status
        """
        self._execute_handler_async(
            p=p, job_id=job_id, data_dict=data_dict, subscriber=subscriber
        )

        while True:
            # TODO: investigate if list_namespaced_job(watch=True) can be used here
            time.sleep(2)
            job = self.get_job(job_id=job_id)
            if not job:
                LOGGER.warning(f"Job {job_id} has vanished")
                status = JobStatus.failed
                break

            status = JobStatus[job["status"]]
            if status not in (JobStatus.running, JobStatus.accepted):
                break

        mimetype, result = self.get_job_result(job_id=job_id)

        return (mimetype, result, status)

    def _execute_handler_async(
        self,
        p: KubernetesProcessor,
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
        job_name = k8s_job_name(job_id=job_id)
        job_pod_spec = p.create_job_pod_spec(
            data=data_dict,
            job_name=job_name,
        )

        annotations = {
            "identifier": job_id,
            "process_id": p.metadata.get("id"),
            "job_start_datetime": now_str(),
            **job_pod_spec.extra_annotations,
        }
        if subscriber:
            if subscriber.success_uri:
                annotations["success-uri"] = subscriber.success_uri
            if subscriber.failed_uri:
                annotations["failed-uri"] = subscriber.failed_uri

        job = k8s_client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                annotations={
                    format_annotation_key(k): v for k, v in annotations.items()
                },
            ),
            spec=k8s_client.V1JobSpec(
                template=k8s_client.V1PodTemplateSpec(
                    metadata=k8s_client.V1ObjectMeta(labels=job_pod_spec.extra_labels),
                    spec=job_pod_spec.pod_spec,
                ),
                backoff_limit=0,
                ttl_seconds_after_finished=60 * 60 * 24 * 100,  # about 3 months
            ),
        )

        self.batch_v1.create_namespaced_job(body=job, namespace=self.namespace)

        LOGGER.info("Add job %s in ns %s", job.metadata.name, self.namespace)

        return ("application/json", {}, JobStatus.accepted)

    def _job_message(self, job: k8s_client.V1Job) -> Optional[str]:
        if job_status_from_k8s(job.status) == JobStatus.accepted:
            # if a job is in state accepted, it means that it can run right now
            # and we the events can show why that is
            events: k8s_client.V1EventList = self.core_api.list_namespaced_event(
                namespace=self.namespace,
                field_selector=(
                    f"involvedObject.name={job.metadata.name},"
                    "involvedObject.kind=Job"
                ),
            )
            if items := events.items:
                return items[-1].message

        if pod := self._pod_for_job(job):
            # everything can be null in kubernetes, even empty lists
            if pod.status.container_statuses:
                state: k8s_client.V1ContainerState = pod.status.container_statuses[
                    0
                ].state
                interesting_states = [s for s in (state.waiting, state.terminated) if s]
                if interesting_states:
                    return ": ".join(
                        filter(
                            None,
                            (
                                interesting_states[0].reason,
                                interesting_states[0].message,
                            ),
                        )
                    )
        return None

    def _pod_for_job(self, job: k8s_client.V1Job) -> Optional[k8s_client.V1Pod]:
        label_selector = ",".join(
            f"{key}={value}" for key, value in job.spec.selector.match_labels.items()
        )
        pods: k8s_client.V1PodList = self.core_api.list_namespaced_pod(
            namespace=self.namespace, label_selector=label_selector
        )

        return next(iter(pods.items), None)


_ANNOTATIONS_PREFIX = "pygeoapi.io/"


def parse_annotation_key(key: str) -> Optional[str]:
    matched = re.match(f"^{_ANNOTATIONS_PREFIX}(.+)", key)
    return matched.group(1) if matched else None


def format_annotation_key(key: str) -> str:
    return _ANNOTATIONS_PREFIX + key


def job_status_from_k8s(status: k8s_client.V1JobStatus) -> JobStatus:
    # we assume only 1 run without retries

    # these "integers" are None if they are 0, lol
    if status.succeeded is not None and status.succeeded > 0:
        return JobStatus.successful
    elif status.failed is not None and status.failed > 0:
        return JobStatus.failed
    elif status.active is not None and status.active > 0:
        return JobStatus.running
    else:
        return JobStatus.accepted


def job_from_k8s(job: k8s_client.V1Job, message: Optional[str]) -> JobDict:
    # annotations is broken in the k8s library, it's None when it is empty
    annotations = job.metadata.annotations or {}
    metadata_from_annotation = {
        parsed_key: v
        for orig_key, v in annotations.items()
        if (parsed_key := parse_annotation_key(orig_key))
    }
    executed_notebook = metadata_from_annotation.get("executed-notebook")

    try:
        metadata_from_annotation["parameters"] = json.dumps(
            hide_secret_values(
                json.loads(
                    metadata_from_annotation.get("parameters", "{}"),
                )
                # executed notebook is not part of params, but show in UI
                | (
                    {"executed-notebook": executed_notebook}
                    if executed_notebook
                    else {}
                ),
            )
        )
    except json.JSONDecodeError:
        LOGGER.info("cant obfuscate parameters, not valid json", exc_info=True)

    status = job_status_from_k8s(job.status)
    completion_time = get_completion_time(job, status)
    # default values in case we don't get them from metadata
    default_progress = "100" if status == JobStatus.successful else "1"

    return cast(
        JobDict,
        {
            # need this key in order not to crash, overridden by metadata:
            "identifier": "",
            "process_id": "",
            "job_start_datetime": "",
            # NOTE: this is passed as string as compatibility with base manager
            "status": status.value,
            "mimetype": None,  # we don't know this in general
            "message": message if message else "",
            "progress": default_progress,
            "job_end_datetime": (
                completion_time.strftime(DATETIME_FORMAT) if completion_time else None
            ),
            **metadata_from_annotation,
        },
    )


def hide_secret_values(d: dict[str, str]) -> dict[str, str]:
    def transform_value(k, v):
        return (
            "*"
            if any(trigger in k.lower() for trigger in ["secret", "key", "password"])
            else v
        )

    return {k: transform_value(k, v) for k, v in d.items()}


def get_completion_time(job: k8s_client.V1Job, status: JobStatus) -> Optional[datetime]:
    if status == JobStatus.failed:
        # failed jobs have special completion time field
        return max(
            (
                condition.last_transition_time
                for condition in job.status.conditions
                if condition.type == "Failed" and condition.status == "True"
            ),
            default=None,
        )

    return job.status.completion_time


def current_namespace():
    # getting the current namespace like this is documented, so it should be fine:
    # https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/
    return open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()


def job_babysitter(namespace: str) -> None:
    while True:
        _send_pending_notifications(namespace=namespace)
        time.sleep(60)


def _send_pending_notifications(namespace: str):
    def _do_send(status: Literal["success", "failed"]):
        batch_v1 = k8s_client.BatchV1Api()

        already_sent_key = format_annotation_key(f"{status}-sent")
        uri_key = format_annotation_key(f"{status}-uri")
        for relevant_job in get_jobs_by_status(namespace, status):
            annotations = relevant_job.metadata.annotations

            if (url := annotations.get(uri_key)) and not annotations.get(
                already_sent_key
            ):
                LOGGER.info(f"Found {status} job {relevant_job.metadata.name}, sending")

                batch_v1.patch_namespaced_job(
                    name=relevant_job.metadata.name,
                    namespace=namespace,
                    body={"metadata": {"annotations": {already_sent_key: now_str()}}},
                )

                requests.post(
                    url,
                    json=job_from_k8s(relevant_job, message=""),
                )

    _do_send(status="success")
    _do_send(status="failed")


def get_jobs_by_status(
    namespace: str,
    status: Literal["success", "failed"],
) -> list[k8s_client.V1Job]:
    batch_v1 = k8s_client.BatchV1Api()

    if status == "success":
        return batch_v1.list_namespaced_job(
            namespace=namespace, field_selector="status.successful==1"
        ).items
    elif status == "failed":
        # k8s doesn't support retrieving failed jobs,
        # we have to implement it ourselves :'(
        # https://github.com/kubernetes/kubernetes/issues/86352
        # https://github.com/kubernetes/kubernetes/pull/87863
        jobs = batch_v1.list_namespaced_job(
            namespace=namespace, field_selector="status.successful==0"
        ).items
        return [
            job for job in jobs if job_status_from_k8s(job.status) == JobStatus.failed
        ]


def now_str() -> str:
    return datetime.now(timezone.utc).strftime(DATETIME_FORMAT)
