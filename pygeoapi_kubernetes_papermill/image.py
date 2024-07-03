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

import copy
from dataclasses import dataclass
import json
import logging
from pathlib import PurePath
from pygeoapi.util import ProcessExecutionMode
from typing import Optional
from typed_json_dataclass import TypedJsonMixin

from kubernetes import client as k8s_client

from .kubernetes import (
    KubernetesProcessor,
)
from .common import (
    ContainerKubernetesProcessorMixin,
    ProcessorClientError,
    drop_none_values,
    setup_byoa_results_dir_cmd,
)


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


class ContainerImageKubernetesProcessor(
    ContainerKubernetesProcessorMixin, KubernetesProcessor
):
    def __init__(self, processor_def: dict) -> None:
        metadata = copy.deepcopy(PROCESS_METADATA)
        # If the process defines this, we are basically in generic modoe
        for generic_process_key in ["id", "title", "version", "inputs"]:
            if generic_process_value := processor_def.get(generic_process_key):
                metadata[generic_process_key] = generic_process_value

        super().__init__(processor_def, metadata)

        self.default_image: str = processor_def["default_image"]
        self.command: str = processor_def["command"]
        self.allowed_images_regex: str = processor_def["allowed_images_regex"]
        # self.image_pull_secret: str = processor_def["image_pull_secret"]
        self.s3: Optional[dict[str, str]] = processor_def.get("s3")
        self.extra_volumes: list = processor_def["extra_volumes"]
        self.extra_volume_mounts: list = processor_def["extra_volume_mounts"]
        self.node_purpose_label_key: str = processor_def["node_purpose_label_key"]
        self.default_node_purpose: str = processor_def["default_node_purpose"]
        self.allowed_node_purposes_regex: str = processor_def[
            "allowed_node_purposes_regex"
        ]
        self.tolerations: list = processor_def["tolerations"]
        self.allow_fargate: bool = processor_def["allow_fargate"]
        self.parameters_env: dict[str, str] = processor_def["parameters_env"]
        self.secrets = processor_def["secrets"]

    def create_job_pod_spec(
        self,
        data: dict,
        job_name: str,
    ) -> KubernetesProcessor.JobPodSpec:
        LOGGER.debug("Starting job with data %s", data)

        try:
            requested = RequestParameters.from_dict(data)
        except (TypeError, KeyError) as e:
            raise ProcessorClientError(user_msg=f"Invalid parameter: {e}") from e

        extra_config = self._extra_configs()
        extra_podspec = self._extra_podspec(requested)

        image_container = k8s_client.V1Container(
            name="notebook",
            image=self.default_image,
            command=[
                "bash",
                "-i",
                "-c",
                (
                    setup_byoa_results_dir_cmd(
                        parent_of_subdir=PurePath("/full-results-pvc"),
                        subdir=requested.result_data_directory,
                        result_data_path=PurePath("/output-byoa"),
                        job_name=job_name,
                    )
                    if requested.result_data_directory
                    else ""
                )
                + self.command,
            ],
            volume_mounts=extra_config.volume_mounts,
            resources=_resource_requirements(requested),
            env=(
                to_k8s_env(requested.parameters_env) if requested.parameters_env else []
            )
            + to_k8s_env(self.parameters_env),
            env_from=extra_config.env_from,
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
                **extra_podspec,
                enable_service_links=False,
            ),
            extra_annotations={
                "parameters": json.dumps(requested.parameters_env)[:8000]
            },
            extra_labels={"runtime": "fargate"} if requested.run_on_fargate else {},
        )

    def __repr__(self):
        return "<ContainerImageKubernetesProcessor> {}".format(self.name)


def _resource_requirements(requested: RequestParameters):
    return k8s_client.V1ResourceRequirements(
        limits=drop_none_values(
            {
                "cpu": requested.cpu_limit,
                "memory": requested.mem_limit,
            }
        ),
        requests=drop_none_values(
            {
                "cpu": requested.cpu_requests,
                "memory": requested.mem_requests,
            }
        ),
    )


def to_k8s_env(env: dict[str, str]) -> list[k8s_client.V1EnvVar]:
    return [
        k8s_client.V1EnvVar(
            name=k,
            value=v,
        )
        for k, v in env.items()
    ]
