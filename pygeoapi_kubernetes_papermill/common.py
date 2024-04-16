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
import functools
import logging
import operator
from typing import Any, Iterable, List, Dict, Optional
import re

from http import HTTPStatus
from pygeoapi.process.base import ProcessorExecuteError


from kubernetes import client as k8s_client


LOGGER = logging.getLogger(__name__)


_JOB_NAME_PREFIX = "pygeoapi-job-"

JOVIAN_UID = 1000
JOVIAN_GID = 100


def k8s_job_name(job_id: str) -> str:
    return f"{_JOB_NAME_PREFIX}{job_id}"


def is_k8s_job_name(job_name: str) -> bool:
    return job_name.startswith(_JOB_NAME_PREFIX)


def job_id_from_job_name(job_name: str) -> str:
    return job_name[len(_JOB_NAME_PREFIX) :]  # noqa


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


def drop_none_values(d: Dict) -> Dict:
    return {k: v for k, v in d.items() if v is not None}


class ContainerKubernetesProcessorMixin:
    allowed_node_purposes_regex: str
    default_node_purpose: str
    node_purpose_label_key: str
    s3: Optional[Dict[str, str]]
    extra_volumes: list
    extra_volume_mounts: list
    allow_fargate: bool
    tolerations: list

    def _extra_podspec(self, requested: Any):
        extra_podspec: Dict[str, Any] = {
            "tolerations": [
                k8s_client.V1Toleration(**toleration) for toleration in self.tolerations
            ]
        }

        if requested.run_on_fargate and not self.allow_fargate:
            raise ProcessorClientError(
                user_msg="run_on_fargate is not allowed on this pygeoapi"
            )

        if not requested.run_on_fargate:
            extra_podspec["affinity"] = self.affinity(requested.node_purpose)

        return extra_podspec

    def affinity(self, requested_node_purpose: Optional[str]) -> k8s_client.V1Affinity:
        if node_purpose := requested_node_purpose:
            if not re.fullmatch(
                self.allowed_node_purposes_regex, requested_node_purpose
            ):
                raise ProcessorClientError(
                    user_msg=f"Node purpose {requested_node_purpose} not allowed, "
                    f"only {self.allowed_node_purposes_regex}"
                )
        else:
            node_purpose = self.default_node_purpose

        node_selector = k8s_client.V1NodeSelector(
            node_selector_terms=[
                k8s_client.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s_client.V1NodeSelectorRequirement(
                            key=self.node_purpose_label_key,
                            operator="In",
                            values=[node_purpose],
                        ),
                    ]
                )
            ]
        )
        return k8s_client.V1Affinity(
            node_affinity=k8s_client.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=node_selector
            )
        )

    def _extra_configs(self) -> ExtraConfig:  # type: ignore
        def extra_configs() -> Iterable[ExtraConfig]:
            yield from (
                extra_volume_config(extra_volume) for extra_volume in self.extra_volumes
            )
            yield from (
                extra_volume_mount_config(extra_volume_mount)
                for extra_volume_mount in self.extra_volume_mounts
            )

            if self.s3:
                yield s3_config(
                    bucket_name=self.s3["bucket_name"],
                    secret_name=self.s3["secret_name"],
                    mount_path=self.s3["mount_path"],
                    s3_url=self.s3["s3_url"],
                )

        return functools.reduce(operator.add, extra_configs(), ExtraConfig())


def extra_volume_config(extra_volume: Dict) -> ExtraConfig:
    # stupid transformer from dict to anemic k8s model
    # NOTE: kubespawner/utils.py has a fancy `get_k8s_model`
    #       which performs the same thing but way more thoroughly.
    #       Trying to avoid this complexity here for now
    def construct_value(k, v):
        if k == "persistentVolumeClaim":
            return k8s_client.V1PersistentVolumeClaimVolumeSource(**build(v))
        else:
            return v

    def build(input_dict: Dict):
        return {
            camel_case_to_snake_case(k): construct_value(k, v)
            for k, v in input_dict.items()
        }

    return ExtraConfig(volumes=[k8s_client.V1Volume(**build(extra_volume))])


def extra_volume_mount_config(extra_volume_mount: Dict) -> ExtraConfig:
    # stupid transformer from dict to anemic k8s model
    def build(input_dict: Dict):
        return {camel_case_to_snake_case(k): v for k, v in input_dict.items()}

    return ExtraConfig(
        volume_mounts=[k8s_client.V1VolumeMount(**build(extra_volume_mount))]
    )


def s3_config(bucket_name, secret_name, s3_url, mount_path) -> ExtraConfig:
    s3_user_bucket_volume_name = "s3-user-bucket"
    return ExtraConfig(
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=mount_path,
                name=s3_user_bucket_volume_name,
                mount_propagation="HostToContainer",
            )
        ],
        volumes=[
            k8s_client.V1Volume(
                name=s3_user_bucket_volume_name,
                empty_dir=k8s_client.V1EmptyDirVolumeSource(),
            )
        ],
        containers=[
            k8s_client.V1Container(
                name="s3mounter",
                image="totycro/s3fs:0.7.0-1.90",
                # we need to detect the end of the job here, this container
                # must end for the job to be considered done by k8s
                # this is a missing feature in k8s:
                # https://github.com/kubernetes/enhancements/issues/753
                args=[
                    "sh",
                    "-c",
                    'echo "`date` waiting for job start"; '
                    # first we wait 3 seconds because we might start before papermill
                    'sleep 3; echo "`date` job start assumed"; '
                    # we can't just check for papermill, because an `ls` happens before,
                    # which in extreme cases can take seconds. so we check for bash,
                    # because the s3fs container doesn't have that and we use that
                    # in the other container. this is far from perfect.
                    "while pgrep -x bash >/dev/null; do sleep 1; done; "
                    'echo "`date` job end detected"; ',
                ],
                security_context=k8s_client.V1SecurityContext(privileged=True),
                volume_mounts=[
                    k8s_client.V1VolumeMount(
                        name=s3_user_bucket_volume_name,
                        mount_path="/opt/s3fs/bucket",
                        mount_propagation="Bidirectional",
                    ),
                ],
                resources=k8s_client.V1ResourceRequirements(
                    limits={"cpu": "0.2", "memory": "512Mi"},
                    requests={
                        "cpu": "0.05",
                        "memory": "32Mi",
                    },
                ),
                env=[
                    k8s_client.V1EnvVar(name="S3FS_ARGS", value="-oallow_other"),
                    k8s_client.V1EnvVar(name="UID", value=str(JOVIAN_UID)),
                    k8s_client.V1EnvVar(name="GID", value=str(JOVIAN_GID)),
                    k8s_client.V1EnvVar(
                        name="AWS_S3_ACCESS_KEY_ID",
                        value_from=k8s_client.V1EnvVarSource(
                            secret_key_ref=k8s_client.V1SecretKeySelector(
                                name=secret_name,
                                key="username",
                            )
                        ),
                    ),
                    k8s_client.V1EnvVar(
                        name="AWS_S3_SECRET_ACCESS_KEY",
                        value_from=k8s_client.V1EnvVarSource(
                            secret_key_ref=k8s_client.V1SecretKeySelector(
                                name=secret_name,
                                key="password",
                            )
                        ),
                    ),
                    k8s_client.V1EnvVar(
                        "AWS_S3_BUCKET",
                        bucket_name,
                    ),
                    # due to the shared process namespace, tini is not PID 1, so:
                    k8s_client.V1EnvVar(name="TINI_SUBREAPER", value="1"),
                    k8s_client.V1EnvVar(
                        name="AWS_S3_URL",
                        value=s3_url,
                    ),
                ],
            )
        ],
    )


def camel_case_to_snake_case(s: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()
