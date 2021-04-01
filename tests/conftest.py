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

import datetime
from kubernetes import client as k8s_client
import pytest
from unittest import mock

# NOTE: this file MUST NOT import the actual code since that would register
#       additional views too early


@pytest.fixture()
def k8s_job() -> k8s_client.V1Job:
    from pygeoapi_kubernetes_papermill.kubernetes import k8s_job_name

    return k8s_client.V1Job(
        spec=k8s_client.V1JobSpec(
            template="",
            selector=k8s_client.V1LabelSelector(match_labels={}),
        ),
        metadata=k8s_client.V1ObjectMeta(
            name=k8s_job_name("test"),
            annotations={
                "pygeoapi.io/result-notebook": "/a/b/a.ipynb",
                "pygeoapi.io/result-link": "https://www.example.com",
            },
        ),
        status=k8s_client.V1JobStatus(
            succeeded=1, completion_time=datetime.datetime(2020, 1, 1, 4, 0)
        ),
    )


@pytest.fixture()
def mock_read_job(k8s_job):
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.BatchV1Api.read_namespaced_job",
        return_value=k8s_job,
    ):
        yield


@pytest.fixture()
def mock_list_pods():
    with mock.patch(
        "pygeoapi_kubernetes_papermill."
        "kubernetes.k8s_client.CoreV1Api.list_namespaced_pod",
        return_value=k8s_client.V1PodList(
            items=[
                k8s_client.V1Pod(
                    status=k8s_client.V1PodStatus(
                        container_statuses=[
                            k8s_client.V1ContainerStatus(
                                image="a",
                                image_id="b",
                                name="c",
                                ready=True,
                                restart_count=0,
                                state=k8s_client.V1ContainerState(
                                    terminated=k8s_client.V1ContainerStateTerminated(
                                        exit_code=0
                                    )
                                ),
                            )
                        ],
                    )
                )
            ]
        ),
    ):
        yield
