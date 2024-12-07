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
import logging
import itertools
import time

from flask import Response
import requests

# NOTE: this assumes flask_app, which is the default.
from pygeoapi.flask_app import APP


LOGGER = logging.getLogger(__name__)


@APP.get("/jobs/<job_id>/logs")
def get_job_logs(job_id):
    LOGGER.info(f"Retrieving job logs for {job_id}")

    from pygeoapi.flask_app import api_
    from pygeoapi_kubernetes_papermill.common import k8s_job_name

    log_query_endpoint = getattr(api_.manager, "log_query_endpoint", None)
    if not log_query_endpoint:
        headers, status, content = api_.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            {},
            "json",
            "NoApplicableCode",
            "logs not configured for this pygeoapi instance",
        )
        return content, status, headers
    else:
        namespace = api_.manager.namespace

        # 21 days in ns (default limit in loki is 30 days)
        query_time_range = 21 * 24 * 60 * 60 * 1_000_000_000

        job_name = k8s_job_name(job_id)

        request_params = {
            "query": f'{{job="{namespace}/{job_name}"}}',
            "start": time.time_ns() - query_time_range,
            "end": time.time_ns(),
        }
        response = requests.get(
            log_query_endpoint,
            params=request_params,
        )
        response.raise_for_status()
        streams = response.json()["data"]["result"]

        log_output = "\n".join(
            entry[1]
            for entry in sorted(
                # here let's just mix stdout/stderr
                itertools.chain.from_iterable(result["values"] for result in streams)
            )
        )
        return Response(log_output, mimetype="text/plain")
