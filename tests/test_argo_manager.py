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

from http import HTTPStatus
from unittest import mock
import json

import requests
import pytest
from kubernetes import client as k8s_client

from pygeoapi.util import JobStatus, RequestedProcessExecutionMode, Subscriber
from pygeoapi_kubernetes_papermill.argo import ArgoProcessor, ArgoManager


@pytest.fixture()
def manager(mock_k8s_base) -> ArgoManager:
    man = ArgoManager(
        {
            "name": "kman",
            "skip_k8s_setup": True,
            "log_query_endpoint": "",
            "results_link_template": "https://example.com/a/{job_id}",
        }
    )
    man.get_processor = lambda *args, **kwargs: ArgoProcessor(
        {"name": "", "workflow_template": "mywf"}
    )
    return man


def test_execute_process_starts_async_job(
    manager: ArgoManager,
    mock_create_workflow,
    mock_get_workflow_template,
):
    job_id = "abc"
    result = manager.execute_process(
        process_id="some-processor",
        desired_job_id=job_id,
        data_dict={"param1": "value1"},
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

    job: k8s_client.V1Job = mock_create_workflow.mock_calls[0][2]["body"]
    assert job["spec"]["arguments"]["parameters"] == [
        {"name": "param1", "value": "value1"}
    ]
    assert job_id in job["metadata"]["name"]

    assert job["metadata"]["annotations"]["pygeoapi.io/identifier"] == job_id
    #  assert (
    #      job.metadata.annotations["pygeoapi.io/success-uri"]
    #      == "https://example.com/success"
    #  )
    #  assert (
    #      job.metadata.annotations["pygeoapi.io/failed-uri"]
    #      == "https://example.com/failed"
    #  )


def test_get_job_returns_workflow(
    manager: ArgoManager,
    mock_get_workflow,
):
    job_id = "abc"
    job = manager.get_job(job_id=job_id)
    assert job
    assert job["identifier"] == "annotations-identifier"
    assert json.loads(job["parameters"]) == {"inpfile": "test2.txt"}  # type: ignore
    assert job.get("job_start_datetime") == "2024-09-18T12:01:02.000000Z"
    assert job["status"] == "successful"


def test_get_jobs_returns_workflows(
    manager: ArgoManager,
    mock_list_workflows,
):
    response = manager.get_jobs()
    assert response["numberMatched"] == 1
    job = response["jobs"][0]
    assert job["identifier"] == "annotations-identifier"


def test_delete_job_deletes_job(
    manager: ArgoManager,
    mock_delete_workflow,
):
    response = manager.delete_job("abc")

    assert response
    mock_delete_workflow.assert_called()


def test_inputs_retrieved_from_workflow_template(
    mock_k8s_base,
    mock_get_workflow_template,
):
    processor = ArgoProcessor({"name": "proc", "workflow_template": "whatever"})
    assert processor.metadata["inputs"]["param"]["minOccurs"] == 1
    assert processor.metadata["inputs"]["param-optional"]["minOccurs"] == 0


@pytest.fixture()
def mock_fetch_job_result():
    response = requests.Response()
    response.status_code = HTTPStatus.OK
    response.headers["content-type"] = "application/json"
    response._content = b'{"a": 3}'

    with mock.patch(
        "pygeoapi_kubernetes_papermill.argo.requests.get",
        return_value=response,
    ) as patcher:
        yield patcher


def test_result_link_is_resolved(manager: ArgoManager, mock_fetch_job_result):
    job_id = "abc-123"
    result = manager.get_job_result(job_id)

    assert result == ("application/json", {"a": 3})

    mock_fetch_job_result.assert_called_with("https://example.com/a/abc-123")


@pytest.fixture()
def mock_create_workflow():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CustomObjectsApi.create_namespaced_custom_object",
        return_value=None,
    ) as mocker:
        yield mocker


@pytest.fixture()
def mock_get_workflow(workflow):
    # NOTE: mocks same function as get_workflow_template
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CustomObjectsApi.get_namespaced_custom_object",
        return_value=workflow,
    ) as mocker:
        yield mocker


@pytest.fixture()
def mock_list_workflows(workflow):
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CustomObjectsApi.list_namespaced_custom_object",
        return_value={"items": [workflow]},
    ) as mocker:
        yield mocker


@pytest.fixture()
def mock_delete_workflow():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CustomObjectsApi.delete_namespaced_custom_object",
    ) as mocker:
        yield mocker


@pytest.fixture()
def mock_get_workflow_template():
    # NOTE: mocks same function as get_workflow
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CustomObjectsApi.get_namespaced_custom_object",
        return_value=MOCK_WORKFLOW_TEMPLATE,
    ) as mocker:
        yield mocker


MOCK_WORKFLOW_TEMPLATE = {
    "metadata": {
        "name": "test",
        "namespace": "test",
    },
    "spec": {
        "imagePullSecrets": [{"name": "flux-cerulean"}],
        "serviceAccountName": "argo-workflow",
        "templates": [
            {
                "container": {},
                "inputs": {
                    "artifacts": [],
                    "parameters": [
                        {"name": "param"},
                        {"name": "param-optional", "value": "a"},
                    ],
                },
                "name": "execute",
            }
        ],
    },
}
