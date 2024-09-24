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

from contextlib import contextmanager
import json
import pytest
from unittest import mock
from kubernetes import client as k8s_client

from pygeoapi.util import JobStatus, RequestedProcessExecutionMode, Subscriber
from pygeoapi_kubernetes_papermill import (
    KubernetesManager,
    PapermillNotebookKubernetesProcessor,
)
from pygeoapi_kubernetes_papermill.kubernetes import (
    job_from_k8s,
    _send_pending_notifications,
)


@contextmanager
def mock_list_jobs_with(*args):
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.BatchV1Api.list_namespaced_job",
        return_value=k8s_client.V1JobList(items=args),
    ):
        yield


@pytest.fixture()
def mock_list_jobs(k8s_job):
    with mock_list_jobs_with(k8s_job):
        yield


@pytest.fixture()
def mock_list_jobs_accepted(k8s_job: k8s_client.V1Job):
    k8s_job.status = k8s_client.V1JobStatus()
    with mock_list_jobs_with(k8s_job):
        yield


@pytest.fixture()
def mock_create_job():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.BatchV1Api.create_namespaced_job",
        return_value=None,
    ) as mocker:
        yield mocker


@pytest.fixture()
def mock_patch_job():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.BatchV1Api.patch_namespaced_job",
    ) as mocker:
        yield mocker


@pytest.fixture()
def mock_list_pods_no_container_status():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CoreV1Api.list_namespaced_pod",
        return_value=k8s_client.V1PodList(
            items=[
                k8s_client.V1Pod(
                    status=k8s_client.V1PodStatus(
                        container_statuses=None,
                    )
                )
            ]
        ),
    ):
        yield


@pytest.fixture()
def mock_delete_pod():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CoreV1Api.delete_namespaced_pod",
    ) as m:
        yield m


@pytest.fixture()
def mock_delete_job():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.BatchV1Api.delete_namespaced_job",
    ) as m:
        yield m


@pytest.fixture()
def mock_list_events():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CoreV1Api.list_namespaced_event",
        return_value=k8s_client.CoreV1EventList(
            items=[
                k8s_client.CoreV1Event(
                    message="first event", involved_object=object(), metadata=object()
                ),
                k8s_client.CoreV1Event(
                    message="last event", involved_object=object(), metadata=object()
                ),
            ]
        ),
    ):
        yield


@pytest.fixture()
def mock_scrapbook_read_notebook():
    with mock.patch(
        "pygeoapi_kubernetes_papermill.notebook.scrapbook.read_notebook",
        return_value=mock.MagicMock(scraps=[]),
    ) as m:
        yield m


@pytest.fixture()
def mock_wait_for_result_file():
    with mock.patch(
        "pygeoapi_kubernetes_papermill.notebook._wait_for_result_file",
    ) as m:
        yield m


@pytest.fixture()
def manager(mock_k8s_base, papermill_processor) -> KubernetesManager:
    man = KubernetesManager({"name": "kman"})
    man.get_processor = lambda *args, **kwargs: papermill_processor
    return man


def test_deleting_job_deletes_in_k8s_and_on_nb_file_on_disc(
    manager: KubernetesManager,
    mock_read_job,
    mock_list_pods,
    mock_delete_job,
    mock_delete_pod,
):
    with mock.patch(
        "pygeoapi_kubernetes_papermill.kubernetes.os.remove"
    ) as mock_os_remove:
        result = manager.delete_job(2)

    assert result
    mock_delete_job.assert_called_once()
    mock_delete_pod.assert_called_once()
    mock_os_remove.assert_called_once()


@pytest.fixture
def papermill_processor() -> PapermillNotebookKubernetesProcessor:
    return PapermillNotebookKubernetesProcessor(
        processor_def={
            "name": "test",
            "s3": None,
            "default_image": "example",
            "allowed_images_regex": "",
            "extra_pvcs": [],
            "extra_volumes": [],
            "extra_volume_mounts": [],
            "home_volume_claim_name": "user",
            "image_pull_secret": "",
            "jupyter_base_url": "",
            "output_directory": "/home/jovyan/tmp",
            "secrets": [],
            "default_node_purpose": "foo",
            "allowed_node_purposes_regex": "",
            "tolerations": [],
            "log_output": False,
            "job_service_account": "job-service-account",
            "allow_fargate": False,
            "auto_mount_secrets": False,
            "node_purpose_label_key": "hub.example.com/node",
            "run_as_user": None,
            "run_as_group": None,
            "conda_store_groups": [],
            "extra_resource_limits": {},
            "extra_resource_requests": {},
        }
    )


def test_execute_process_starts_async_job(
    manager: KubernetesManager,
    mock_create_job,
):
    job_id = "abc"
    result = manager.execute_process(
        process_id="papermill-processor",
        desired_job_id=job_id,
        data_dict={"notebook": "a.ipynb"},
        subscriber=Subscriber(
            success_uri="https://example.com/success",
            failed_uri="https://example.com/failed",
            in_progress_uri=None,
        ),
        execution_mode=RequestedProcessExecutionMode.respond_async,
    )
    assert result == (
        "abc",
        "application/json",
        {},
        JobStatus.accepted,
        {"Preference-Applied": "respond-async"},
    )

    job: k8s_client.V1Job = mock_create_job.mock_calls[0][2]["body"]
    assert job_id in job.metadata.name
    assert job.metadata.annotations["pygeoapi.io/identifier"] == job_id
    assert (
        job.metadata.annotations["pygeoapi.io/success-uri"]
        == "https://example.com/success"
    )
    assert (
        job.metadata.annotations["pygeoapi.io/failed-uri"]
        == "https://example.com/failed"
    )


def test_get_jobs_handles_container_status_null(
    manager: KubernetesManager,
    mock_list_jobs,
    mock_list_pods_no_container_status,
):
    # NOTE: this test could be reduced to only test _job_message() if the excessive
    #       mocking causes issues
    job_data = manager.get_jobs()
    assert [job["message"] for job in job_data["jobs"]] == [""]


def test_execute_process_sync_also_returns_mime_type(
    manager: KubernetesManager,
    papermill_processor,
    mock_create_job,
    mock_read_job,
    mock_list_pods,
    mock_scrapbook_read_notebook,
    mock_wait_for_result_file,
):
    job_id = "abc"
    actual_job_id, mime, payload, status, headers = manager.execute_process(
        process_id="papermill-processor",
        desired_job_id=job_id,
        data_dict={"notebook": "a.ipynb"},
        execution_mode=RequestedProcessExecutionMode.wait,
    )

    assert actual_job_id == job_id
    assert mime is None
    assert payload == {}
    assert status == JobStatus.successful


def test_accepted_jobs_show_events(
    mock_list_jobs_accepted,
    mock_list_events,
    manager: KubernetesManager,
):
    job_data = manager.get_jobs()

    assert job_data["jobs"][0]["message"] == "last event"


def test_secret_job_annotation_parameters_are_hidden():
    job = k8s_client.V1Job(
        metadata=k8s_client.V1ObjectMeta(
            annotations={
                "pygeoapi.io/parameters": '{"foo": "bar", "foo-secret": "bar"}',
            }
        ),
        status=k8s_client.V1JobStatus(),
    )
    job_dict = job_from_k8s(job, message="")
    parameters = json.loads(job_dict["parameters"])
    assert parameters["foo"] == "bar"
    assert parameters["foo-secret"] == "*"


def test_job_params_contain_executed_notebook():
    job = k8s_client.V1Job(
        metadata=k8s_client.V1ObjectMeta(
            annotations={"pygeoapi.io/executed-notebook": "extra/nb.ipynb"}
        ),
        status=k8s_client.V1JobStatus(),
    )
    job_dict = job_from_k8s(job, message="")
    parameters = json.loads(job_dict["parameters"])
    assert parameters["executed-notebook"] == "extra/nb.ipynb"


def test_successful_job_has_100_progress():
    job = k8s_client.V1Job(
        metadata=k8s_client.V1ObjectMeta(),
        status=k8s_client.V1JobStatus(succeeded=1),
    )
    job_dict = job_from_k8s(job, message="")
    assert job_dict["progress"] == "100"


def test_success_notification_is_sent_for_successful_job(k8s_job, mock_patch_job):
    k8s_job.metadata.annotations["pygeoapi.io/success-uri"] = "https://www.example.com"
    with mock_list_jobs_with(k8s_job), mock.patch(
        "pygeoapi_kubernetes_papermill.kubernetes.requests.post"
    ) as mock_post:
        _send_pending_notifications("mynamespace")

    mock_post.assert_called_once()
    mock_patch_job.assert_called_once()


def test_success_notification_only_sent_once(k8s_job):
    k8s_job.metadata.annotations["pygeoapi.io/success-uri"] = "https://www.example.com"
    k8s_job.metadata.annotations["pygeoapi.io/success-sent"] = "something"
    with mock_list_jobs_with(k8s_job), mock.patch(
        "pygeoapi_kubernetes_papermill.kubernetes.requests.post"
    ) as mock_post:
        _send_pending_notifications("mynamespace")

    mock_post.assert_not_called()


def test_failure_notification_is_sent_for_failing_job(k8s_job_failed, mock_patch_job):
    k8s_job_failed.metadata.annotations["pygeoapi.io/failed-uri"] = (
        "https://www.example.com"
    )
    with mock_list_jobs_with(k8s_job_failed), mock.patch(
        "pygeoapi_kubernetes_papermill.kubernetes.requests.post"
    ) as mock_post:
        _send_pending_notifications("mynamespace")

    mock_post.assert_called_once()
    mock_patch_job.assert_called_once()


def test_kubernetes_manager_handles_pagination(
    manager: KubernetesManager,
    mock_list_pods_no_container_status,
    many_k8s_jobs,
):
    with mock_list_jobs_with(*many_k8s_jobs):
        job_data = manager.get_jobs(offset=3, limit=2)

    jobs = job_data["jobs"]
    assert len(jobs) == 2
    assert [job["identifier"] for job in jobs] == ["job-3", "job-4"]
    assert job_data["numberMatched"] == 13
