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

import logging

from kubernetes import client as k8s_client
from pygeoapi.util import JobStatus
from pygeoapi_kubernetes_papermill.kubernetes import JobDict, k8s_job_name
import requests

# NOTE: this assumes flask_app, which is the default.
from pygeoapi.flask_app import BLUEPRINT, APP


LOGGER = logging.getLogger(__name__)

# TODO: set as config option
QUERY_ENDPOINT = "http://prometheus.kubeprod.svc.cluster.local:9090/api/v1/query"


@BLUEPRINT.route("/processes/<process_id>/jobs/<job_id>/resources")
def get_job_resources(process_id, job_id):

    LOGGER.debug(f"Retrieving job resources for {process_id} {job_id}")

    from pygeoapi.flask_app import api_

    # query prometheus for all pods of job (should be just 1)
    pod_list: k8s_client.V1PodList = k8s_client.CoreV1Api().list_namespaced_pod(
        namespace=api_.manager.namespace,
        label_selector=f"job-name={k8s_job_name(job_id)}",
    )
    pod_selector = "|".join(pod.metadata.name for pod in pod_list.items)

    metrics_selector = ",".join(
        (
            f'namespace="{api_.manager.namespace}"',
            f'pod=~"{pod_selector}"',
            'container="notebook"',
        )
    )

    # we query the max mem of this container for the last year (to cover whole runtime)
    query_max_mem = (
        f"max_over_time(container_memory_working_set_bytes{{ {metrics_selector} }}[1y])"
    )
    # for cpu, we can just take the total
    query_cpu = f"container_cpu_usage_seconds_total{{ {metrics_selector} }}"

    job: JobDict = api_.manager.get_job(job_id=job_id)

    if job["status"] != JobStatus.successful.value:
        return (f"Job is {job['status']}", 400)
    else:
        time = job["job_end_datetime"]
        return {
            "max_mem_bytes": query_prometheus(query=query_max_mem, time=time),
            "cpu_seconds": query_prometheus(query=query_cpu, time=time),
        }


def query_prometheus(query: str, time: str) -> int:

    query_params = {"query": query, "time": time}
    APP.logger.info(f"Querying prometheus {QUERY_ENDPOINT} {query_params}")
    response = requests.get(QUERY_ENDPOINT, params=query_params)

    response.raise_for_status()
    response_data = response.json()
    if response_data.get("status") != "success":
        raise Exception("Invalid response status")

    # only 1 result metric expected
    (result,) = response_data["data"]["result"]
    # value[0] is timestamp, value[1] is actual value, but as string
    # our values are large (seconds or bytes), so we don't care about decimal places
    return int(float(result["value"][1]))
