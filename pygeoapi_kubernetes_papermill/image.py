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

from dataclasses import dataclass
import logging
from pygeoapi.util import ProcessExecutionMode
from typing import Dict, Optional, List
from typed_json_dataclass import TypedJsonMixin

from kubernetes import client as k8s_client

from .kubernetes import (
    KubernetesProcessor,
)
from .common import ExtraConfig, ProcessorClientError


LOGGER = logging.getLogger(__name__)


#: Process metadata and description
PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "execute-image",
    "title": "Execute custom container images",
    "description": "",
    "keywords": [""],
    "links": [],
    "inputs": {
        # TODO
    },
    "outputs": {},
    "example": {},
    "jobControlOptions": [
        ProcessExecutionMode.async_execute.value,
        ProcessExecutionMode.sync_execute.value,
    ],
}


@dataclass(frozen=True)
class RequestParameters(TypedJsonMixin):
    parameters_env: Optional[dict] = None
    cpu_limit: Optional[str] = None
    mem_limit: Optional[str] = None
    cpu_requests: Optional[str] = None
    mem_requests: Optional[str] = None
    result_data_directory: Optional[str] = None
    run_on_fargate: Optional[bool] = False
    node_purpose: Optional[str] = ""


class ContainerImageKubernetesProcessor(KubernetesProcessor):
    def __init__(self, processor_def: dict) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

        self.default_image: str = processor_def["default_image"]
        self.command: str = processor_def["command"]
        self.allowed_images_regex: str = processor_def["allowed_images_regex"]
        # self.image_pull_secret: str = processor_def["image_pull_secret"]
        self.s3: Optional[Dict[str, str]] = processor_def.get("s3")
        self.extra_volumes: List = processor_def["extra_volumes"]
        self.extra_volume_mounts: List = processor_def["extra_volume_mounts"]
        self.default_node_purpose: str = processor_def["default_node_purpose"]
        self.allowed_node_purposes_regex: str = processor_def[
            "allowed_node_purposes_regex"
        ]
        self.tolerations: list = processor_def["tolerations"]
        self.allow_fargate: bool = processor_def["allow_fargate"]
        self.parameters_env: dict[str, str] = processor_def["parameters_env"]

    def create_job_pod_spec(
        self,
        data: Dict,
        job_name: str,
    ) -> KubernetesProcessor.JobPodSpec:
        LOGGER.debug("Starting job with data %s", data)

        try:
            requested = RequestParameters.from_dict(data)
        except (TypeError, KeyError) as e:
            raise ProcessorClientError(user_msg=f"Invalid parameter: {e}") from e

        extra_config = ExtraConfig()
        # TODO

        image_container = k8s_client.V1Container(
            name="notebook",
            image=self.default_image,
            volume_mounts=extra_config.volume_mounts,
            # resources=self._resource_requirements(requested),
            env=(
                to_k8s_env(requested.parameters_env) if requested.parameters_env else []
            )
            + to_k8s_env(self.parameters_env),
        )

        return KubernetesProcessor.JobPodSpec(
            pod_spec=k8s_client.V1PodSpec(
                restart_policy="Never",
                # NOTE: first container is used for status check
                containers=[image_container] + extra_config.containers,
                init_containers=extra_config.init_containers,
                volumes=extra_config.volumes,
                # we need this to be able to terminate the sidecar container
                # https://github.com/kubernetes/kubernetes/issues/25908
                share_process_namespace=True,
                # **extra_podspec,
                enable_service_links=False,
            ),
            extra_annotations={},
            extra_labels={"runtime": "fargate"} if requested.run_on_fargate else {},
        )


def to_k8s_env(env: Dict[str, str]) -> List[k8s_client.V1EnvVar]:
    return [
        k8s_client.V1EnvVar(
            name=k,
            value=v,
        )
        for k, v in env.items()
    ]
