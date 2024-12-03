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
import pytest
import json

import requests


@pytest.fixture()
def mock_load_openapi():
    # we really don't need this, but it's thightly coupled
    with mock.patch("pygeoapi.openapi"):
        yield


@pytest.fixture()
def mock_loki_request():
    response = requests.Response()
    response.status_code = HTTPStatus.OK
    response._content = json.dumps(LOKI_MOCK_RESPONSE).encode()

    with mock.patch(
        "pygeoapi_kubernetes_papermill.log_view.requests.get",
        return_value=response,
    ) as patcher:
        yield patcher


@pytest.fixture
def client():
    from pygeoapi.flask_app import APP

    APP.config["TESTING"] = True
    import os

    os.environ["PYGEOAPI_OPENAPI"] = "foo"
    with APP.test_request_context():
        with APP.test_client() as client:
            yield client


def test_log_view_returns_log_lines(client, mock_loki_request):
    job_id = "abc-123"

    response = client.get(f"/jobs/{job_id}/logs")

    assert (
        mock_loki_request.mock_calls[0][2]["params"]["query"]
        == '{job="test/pygeoapi-job-abc-123"}'
    )

    assert response.status_code == 200
    assert len(response.text.splitlines()) == 4
    assert response.text.startswith('{"event": "HTTP Request: GET https://exampl')


LOKI_MOCK_RESPONSE = {
    "status": "success",
    "data": {
        "resultType": "streams",
        "result": [
            {
                "stream": {
                    "pod": "myservice-6ff9459566-87q7c",
                    "stream": "stderr",
                    "app": "myservice",
                    "container": "myservice",
                    "filename": "/var/log/pods/myservice_myservice",
                    "job": "myservice/myservice",
                    "namespace": "myservice",
                    "node_name": "192.168.0.63",
                },
                "values": [
                    [
                        "1732608728234328263",
                        '{"headers": {"X-Request-ID": "1369ab3ceba2d5d4cbf963d',
                    ],
                    [
                        "1732608728233384370",
                        '{"event": "HTTP Request: POST http://myservice/image/'
                        '\\"HTTP/1.1 200 OK\\""}',
                    ],
                    [
                        "1732608725637512670",
                        '{"event": "HTTP Request: POST http://myservice/image/'
                        '\\"HTTP/1.1 200 OK\\""}',
                    ],
                    [
                        "1732608724453929363",
                        '{"event": "HTTP Request: GET https://example.com/collec',
                    ],
                ],
            }
        ],
        "stats": {
            "summary": {
                "bytesProcessedPerSecond": 53904,
                "linesProcessedPerSecond": 234,
                "totalBytesProcessed": 1378,
                "totalLinesProcessed": 6,
                "execTime": 0.025563608,
                "queueTime": 0.000036888,
                "subqueries": 1,
                "totalEntriesReturned": 6,
            },
            "querier": {
                "store": {
                    "totalChunksRef": 0,
                    "totalChunksDownloaded": 0,
                    "chunksDownloadTime": 0,
                    "chunk": {
                        "headChunkBytes": 0,
                        "headChunkLines": 0,
                        "decompressedBytes": 0,
                        "decompressedLines": 0,
                        "compressedBytes": 0,
                        "totalDuplicates": 0,
                    },
                }
            },
            "ingester": {
                "totalReached": 1,
                "totalChunksMatched": 1,
                "totalBatches": 1,
                "totalLinesSent": 6,
                "store": {
                    "totalChunksRef": 0,
                    "totalChunksDownloaded": 0,
                    "chunksDownloadTime": 0,
                    "chunk": {
                        "headChunkBytes": 0,
                        "headChunkLines": 0,
                        "decompressedBytes": 1378,
                        "decompressedLines": 6,
                        "compressedBytes": 598,
                        "totalDuplicates": 0,
                    },
                },
            },
        },
    },
}
