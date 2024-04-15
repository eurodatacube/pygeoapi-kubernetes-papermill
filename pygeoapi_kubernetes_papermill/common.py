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

from dataclasses import dataclass, field
import logging
from typing import List

from dataclasses import dataclass, field
from http import HTTPStatus
import logging
from pygeoapi.process.base import ProcessorExecuteError
from typing import List

from kubernetes import client as k8s_client


from kubernetes import client as k8s_client


LOGGER = logging.getLogger(__name__)


_JOB_NAME_PREFIX = "pygeoapi-job-"


def k8s_job_name(job_id: str) -> str:
    return f"{_JOB_NAME_PREFIX}{job_id}"


def is_k8s_job_name(job_name: str) -> bool:
    return job_name.startswith(_JOB_NAME_PREFIX)


def job_id_from_job_name(job_name: str) -> str:
    return job_name[len(_JOB_NAME_PREFIX) :]


@dataclass(frozen=True)
class ExtraConfig:
    init_containers: List[k8s_client.V1Container] = field(default_factory=list)
    containers: List[k8s_client.V1Container] = field(default_factory=list)
    volume_mounts: List[k8s_client.V1VolumeMount] = field(default_factory=list)
    volumes: List[k8s_client.V1Volume] = field(default_factory=list)
    env_from: List[k8s_client.V1EnvFromSource] = field(default_factory=list)

    def __add__(self, other):
        return ExtraConfig(
            init_containers=self.init_containers + other.init_containers,
            containers=self.containers + other.containers,
            volume_mounts=self.volume_mounts + other.volume_mounts,
            volumes=self.volumes + other.volumes,
            env_from=self.env_from + other.env_from,
        )


class ProcessorClientError(ProcessorExecuteError):
    http_status_code = HTTPStatus.BAD_REQUEST
