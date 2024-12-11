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

import kubernetes.client.rest
from kubernetes import client as k8s_client
from flask import Response
import requests

# NOTE: this assumes flask_app, which is the default.
from pygeoapi.flask_app import APP
from pygeoapi_kubernetes_papermill.argo import (
    K8S_CUSTOM_OBJECT_WORKFLOWS,
    job_from_k8s_wf,
)
from pygeoapi_kubernetes_papermill.common import parse_pygeoapi_datetime


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

        try:
            k8s_wf: dict = k8s_client.CustomObjectsApi().get_namespaced_custom_object(
                **K8S_CUSTOM_OBJECT_WORKFLOWS,
                name=k8s_job_name(job_id=job_id),
                namespace=namespace,
            )
        except kubernetes.client.rest.ApiException as e:
            if e.status == HTTPStatus.NOT_FOUND:
                return f"Job {job_id} not found"
            else:
                raise

        job_dict = job_from_k8s_wf(k8s_wf)
        job_start = parse_pygeoapi_datetime(job_dict["job_start_datetime"])
        job_start_ns_unix_time = int(job_start.timestamp() * 1_000_000_000)

        # 21 days in ns (default limit in loki is 30 days)
        query_time_range = 21 * 24 * 60 * 60 * 1_000_000_000

        job_name = k8s_job_name(job_id)

        request_params = {
            "query": f'{{job="{namespace}/{job_name}"}}',
            "start": job_start_ns_unix_time,
            "end": job_start_ns_unix_time + query_time_range,
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
