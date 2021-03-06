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

from pygeoapi.util import JobStatus
import pytest
from unittest import mock
from kubernetes import client as k8s_client

from pygeoapi_kubernetes_papermill import (
    KubernetesManager,
    PapermillNotebookKubernetesProcessor,
)


@pytest.fixture()
def mock_k8s_base():
    with mock.patch("pygeoapi_kubernetes_papermill.kubernetes.k8s_config"):
        with mock.patch("pygeoapi_kubernetes_papermill.kubernetes.current_namespace"):
            yield


@pytest.fixture()
def mock_list_jobs(k8s_job):
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.BatchV1Api.list_namespaced_job",
        return_value=k8s_client.V1JobList(items=[k8s_job]),
    ):
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
def mock_delete_job():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.BatchV1Api.delete_namespaced_job",
    ) as m:
        yield m


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
def manager(mock_k8s_base) -> KubernetesManager:
    return KubernetesManager({"name": "kman"})


def test_deleting_job_deletes_in_k8s_and_on_nb_file_on_disc(
    manager: KubernetesManager,
    mock_read_job,
    mock_list_pods,
    mock_delete_job,
):
    with mock.patch(
        "pygeoapi_kubernetes_papermill.kubernetes.os.remove"
    ) as mock_os_remove:
        result = manager.delete_job(1, 2)

    assert result
    mock_delete_job.assert_called_once()
    mock_os_remove.assert_called_once()


@pytest.fixture
def papermill_processor() -> PapermillNotebookKubernetesProcessor:
    return PapermillNotebookKubernetesProcessor(
        processor_def={
            "name": "test",
            "s3": None,
            "default_image": "example",
            "extra_pvcs": [],
            "home_volume_claim_name": "user",
            "image_pull_secret": "",
            "jupyter_base_url": "",
            "output_directory": "/home/jovyan/tmp",
            "secrets": [],
            "log_output": False,
        }
    )


def test_execute_process_starts_async_job(
    manager: KubernetesManager,
    papermill_processor,
    mock_create_job,
):
    job_id = "abc"
    result = manager.execute_process(
        p=papermill_processor,
        job_id=job_id,
        data_dict={"notebook": "a.ipynb"},
        is_async=True,
    )
    assert result == (None, None, JobStatus.accepted)

    job: k8s_client.V1Job = mock_create_job.mock_calls[0][2]["body"]
    assert job_id in job.metadata.name
    assert job.metadata.annotations["pygeoapi.io/identifier"] == job_id


def test_get_jobs_handles_container_status_null(
    manager: KubernetesManager,
    mock_list_jobs,
    mock_list_pods_no_container_status,
):
    # NOTE: this test could be reduced to only test _job_message() if the excessive
    #       mocking causes issues
    jobs = manager.get_jobs()
    assert [job["message"] for job in jobs] == [""]


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
    mime, payload, status = manager.execute_process(
        p=papermill_processor,
        job_id=job_id,
        data_dict={"notebook": "a.ipynb"},
        is_async=False,
    )

    assert mime is None
    assert payload == {"result-link": "https://www.example.com"}
    assert status == JobStatus.successful
