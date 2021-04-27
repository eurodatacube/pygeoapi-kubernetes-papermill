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

import json
import pytest
from unittest import mock
import requests

from pygeoapi.flask_app import APP


@pytest.fixture
def client():
    APP.config["TESTING"] = True

    with APP.test_request_context():
        with APP.test_client() as client:
            yield client


@pytest.fixture()
def mock_prometheus_call():
    def my_prometheus(*args, **kwargs):
        response = requests.models.Response()
        response.status_code = 200
        response._content = json.dumps(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {},
                            # NOTE: this is the format for instant-queries
                            "value": [
                                1616670308.781,
                                "123.123",
                            ],
                        }
                    ],
                },
            }
        ).encode()
        return response

    with mock.patch(
        "pygeoapi_kubernetes_papermill.job_resources.requests.get",
        side_effect=my_prometheus,
    ) as m:
        yield m


def test_job_resources_returns_cpu_and_memory(
    client, mock_read_job, mock_list_pods, mock_prometheus_call
):
    job_id = "123-abc"
    response = client.get(f"/processes/my-process/jobs/{job_id}/resources")

    prom_queries = [call[2]["params"] for call in mock_prometheus_call.mock_calls]
    assert len(prom_queries) == 2
    assert all('pod=~"pod-of-job-123"' in q["query"] for q in prom_queries)
    assert all("2020-01-01T04:00:00.000000Z" == q["time"] for q in prom_queries)

    assert response.status_code == 200
    assert response.json == {"cpu_seconds": 123, "max_mem_bytes": 123}
