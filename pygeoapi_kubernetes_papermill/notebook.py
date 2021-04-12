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

from base64 import b64encode, b64decode
from dataclasses import dataclass, field
from datetime import datetime
import functools
import json
import logging
import mimetypes
import operator
from pathlib import PurePath, Path
import os
import re
import time
from pygeoapi.process.base import ProcessorExecuteError
import scrapbook
import scrapbook.scraps
from typing import Dict, Iterable, Optional, List, Tuple, Any
from typed_json_dataclass import TypedJsonMixin
import urllib.parse

from kubernetes import client as k8s_client

from .kubernetes import (
    JobDict,
    KubernetesProcessor,
    format_annotation_key,
)

LOGGER = logging.getLogger(__name__)


#: Process metadata and description
PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "execute-notebook",
    "title": "notebooks on kubernetes with papermill",
    "description": "",
    "keywords": ["notebook"],
    "links": [
        {
            "type": "text/html",
            "rel": "canonical",
            "title": "eurodatacube",
            "href": "https://eurodatacube.com",
            "hreflang": "en-US",
        }
    ],
    "inputs": [
        {
            "id": "notebook",
            "title": "notebook file (path relative to home)",
            "abstract": "notebook file",
            "input": {
                "literalDataDomain": {
                    "dataType": "string",
                    "valueDefinition": {"anyValue": True},
                }
            },
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,  # TODO how to use?
            "keywords": [""],
        },
        {
            "id": "parameters",
            "title": "parameters (base64 encoded yaml)",
            "abstract": "parameters for notebook execution.",
            "input": {
                "literalDataDomain": {
                    "dataType": "string",
                    "valueDefinition": {"anyValue": True},
                }
            },
            "minOccurs": 0,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [""],
        },
    ],
    "outputs": [
        {
            "id": "result_link",
            "title": "Link to result notebook",
            "description": "Link to result notebook",
            "output": {"formats": [{"mimeType": "text/plain"}]},
        }
    ],
    "example": {},
}


CONTAINER_HOME = Path("/home/jovyan")
S3_MOUNT_PATH = CONTAINER_HOME / "s3"
S3_MOUNT_UID = "1000"
S3_MOUNT_GID = "100"
# we wait for the s3 mount (couldn't think of something better than this):
# * first mount point is emptyDir owned by root.
# * then it will be chowned to user by s3fs bash script, and finally also chowned
#   to group by s3fs itself.
# so when both uid and gid are set up, we should be good to go
# however the mount may fail, in which case we don't want to wait forever,
# so we count attempts
S3_MOUNT_WAIT_CMD = (
    "ATTEMPTS=0; "
    "while "
    f" [ \"$(stat -c '%u %g' '{S3_MOUNT_PATH}')\" != '{S3_MOUNT_UID} {S3_MOUNT_GID}' ] "
    " && [ $((ATTEMPTS++)) -lt 1000 ] "
    "; do echo 'wait for s3 mount'; sleep 0.05 ; done &&"
    ' echo "mount after $ATTEMPTS attempts" && '
)

# NOTE: git checkout container needs a dir for volume and a nested dir for checkout
GIT_CHECKOUT_PATH = CONTAINER_HOME / "git" / "algorithm"

# NOTE: this is not where we store result notebooks (job-output), but where the algorithms
#       should store their result data
RESULT_DATA_PATH = PurePath("/home/jovyan/result-data")

# this just needs to be any unique id
JOB_RUNNER_GROUP_ID = 20200


@dataclass(frozen=True)
class ExtraConfig:
    init_containers: List[k8s_client.V1Container] = field(default_factory=list)
    containers: List[k8s_client.V1Container] = field(default_factory=list)
    volume_mounts: List[k8s_client.V1VolumeMount] = field(default_factory=list)
    volumes: List[k8s_client.V1Volume] = field(default_factory=list)

    def __add__(self, other):
        return ExtraConfig(
            init_containers=self.init_containers + other.init_containers,
            containers=self.containers + other.containers,
            volume_mounts=self.volume_mounts + other.volume_mounts,
            volumes=self.volumes + other.volumes,
        )


@dataclass(frozen=True)
class RequestParameters(TypedJsonMixin):
    notebook: PurePath
    kernel: str
    output_filename: str
    parameters: str = ""
    cpu_limit: Optional[str] = None
    mem_limit: Optional[str] = None
    cpu_requests: Optional[str] = None
    mem_requests: Optional[str] = None
    result_data_directory: Optional[str] = None
    git_revision: Optional[str] = None

    @classmethod
    def from_dict(cls, data) -> "RequestParameters":
        # translate from json to base64
        if (parameters_json := data.pop("parameters_json", None)) :  # noqa
            data["parameters"] = b64encode(
                json.dumps(parameters_json).encode()
            ).decode()

        output_filename_str = data.get(
            "output_filename",
            default_output_path(data["notebook"]),
        )
        data["output_filename"] = PurePath(output_filename_str).name
        data["notebook"] = PurePath(data["notebook"])
        data["result_data_directory"] = data.get("result_data_directory")

        # don't use TypedJsonMixin.from_dict, return type is wrong
        return cls(**data)


class PapermillNotebookKubernetesProcessor(KubernetesProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

        # TODO: config file parsing (typed-json-dataclass?)
        self.default_image: str = processor_def["default_image"]
        self.image_pull_secret: str = processor_def["image_pull_secret"]
        self.s3: Optional[Dict[str, str]] = processor_def.get("s3")
        self.home_volume_claim_name: str = processor_def["home_volume_claim_name"]
        self.extra_pvcs: List = processor_def["extra_pvcs"]
        self.jupyer_base_url: str = processor_def["jupyter_base_url"]
        self.output_directory: Path = Path(processor_def["output_directory"])
        self.secrets = processor_def["secrets"]
        self.checkout_git_repo: Optional[Dict] = processor_def.get("checkout_git_repo")

    def create_job_pod_spec(
        self,
        data: Dict,
        job_name: str,
    ) -> KubernetesProcessor.JobPodSpec:
        LOGGER.debug("Starting job with data %s", data)

        # TODO: allow override from parameter, possibly restrict
        image = self.default_image
        image_name = image.split(":")[0]

        default_kernel = {
            "eurodatacube/jupyter-user": "edc",
            "eurodatacube/jupyter-user-g": "edc-gpu",
        }.get(image_name, "")
        data.setdefault("kernel", default_kernel)

        try:
            requested = RequestParameters.from_dict(data)
        except (TypeError, KeyError) as e:
            raise ProcessorExecuteError(str(e)) from e

        output_notebook = setup_output_notebook(
            output_directory=self.output_directory,
            output_notebook_filename=requested.output_filename,
        )

        is_gpu = image_name == "eurodatacube/jupyter-user-g"
        extra_podspec = gpu_extra_podspec() if is_gpu else {}

        if self.image_pull_secret:
            extra_podspec["image_pull_secrets"] = [
                k8s_client.V1LocalObjectReference(name=self.image_pull_secret)
            ]

        extra_config = self._extra_configs(git_revision=requested.git_revision)

        notebook_container = k8s_client.V1Container(
            name="notebook",
            image=image,
            command=[
                "bash",
                # NOTE: we pretend that the shell is interactive such that it
                #       sources /etc/bash.bashrc which activates the default conda
                #       env. This is ok because the default interactive shell must
                #       have PATH set up to include papermill since regular user
                #       should also be able to execute default env commands without extra
                #       setup
                "-i",
                "-c",
                (S3_MOUNT_WAIT_CMD if self.s3 else "")
                + (
                    s3_subdir_cmd(requested.result_data_directory, job_name)
                    if requested.result_data_directory
                    else ""
                )
                +
                # TODO: weird bug: removing this ls results in a PermissionError when
                #       papermill later writes to the file. This only happens sometimes,
                #       but when it occurs, it does so consistently. I'm leaving that in
                #       for now since that command doesn't do any harm.
                #       (it will be a problem if there are ever a lot of output files,
                #       especially on s3fs)
                f"ls -la {self.output_directory} >/dev/null && "
                f"papermill "
                f'"{requested.notebook}" '
                f'"{output_notebook}" '
                "--engine kubernetes_job_progress "
                f'--cwd "{working_dir(requested.notebook)}" '
                + (f"-k {requested.kernel} " if requested.kernel else "")
                + (f'-b "{requested.parameters}" ' if requested.parameters else ""),
            ],
            working_dir=str(CONTAINER_HOME),
            volume_mounts=extra_config.volume_mounts,
            resources=_resource_requirements(requested),
            env=[
                # this is provided in jupyter worker containers and we also use it
                # for compatibility checks
                k8s_client.V1EnvVar(name="JUPYTER_IMAGE", value=image),
                k8s_client.V1EnvVar(name="JOB_NAME", value=job_name),
                k8s_client.V1EnvVar(
                    name="PROGRESS_ANNOTATION", value=format_annotation_key("progress")
                ),
            ],
        )

        # NOTE: this link currently doesn't work (even those created in
        #   the ui with "create sharable link" don't)
        #   there is a recently closed issue about it:
        # https://github.com/jupyterlab/jupyterlab/issues/8359
        #   it doesn't say when it was fixed exactly. there's a possibly
        #   related fix from last year:
        # https://github.com/jupyterlab/jupyterlab/pull/6773
        result_link = (
            f"{self.jupyer_base_url}/hub/user-redirect/lab/tree/"
            + urllib.parse.quote(str(output_notebook.relative_to(CONTAINER_HOME)))
        )

        # save parameters but make sure the string is not too long
        extra_annotations = {
            "parameters": b64decode(requested.parameters).decode()[:8000],
            "result-link": result_link,
            "result-notebook": str(output_notebook),
        }

        return KubernetesProcessor.JobPodSpec(
            pod_spec=k8s_client.V1PodSpec(
                restart_policy="Never",
                # NOTE: first container is used for status check
                containers=[notebook_container] + extra_config.containers,
                init_containers=extra_config.init_containers,
                volumes=extra_config.volumes,
                # we need this to be able to terminate the sidecar container
                # https://github.com/kubernetes/kubernetes/issues/25908
                share_process_namespace=True,
                service_account="pygeoapi-eoxhub-job",
                security_context=k8s_client.V1PodSecurityContext(
                    supplemental_groups=[JOB_RUNNER_GROUP_ID]
                ),
                **extra_podspec,
            ),
            extra_annotations=extra_annotations,
        )

    def _extra_configs(self, git_revision: Optional[str]) -> ExtraConfig:
        def extra_configs() -> Iterable[ExtraConfig]:
            if self.home_volume_claim_name:
                yield home_volume_config(self.home_volume_claim_name)

            yield from (
                extra_pvc_config(extra_pvc={**extra_pvc, "num": num})
                for num, extra_pvc in enumerate(self.extra_pvcs)
            )

            if self.s3:
                yield s3_config(
                    bucket_name=self.s3["bucket_name"],
                    secret_name=self.s3["secret_name"],
                    s3_url=self.s3["s3_url"],
                )

            yield from (
                extra_secret_config(secret_name=secret["name"], num=num)
                for num, secret in enumerate(self.secrets)
            )

            if self.checkout_git_repo:
                yield git_checkout_config(
                    git_revision=git_revision,
                    **self.checkout_git_repo,
                )

        return functools.reduce(operator.add, extra_configs(), ExtraConfig())

    def __repr__(self):
        return "<PapermillNotebookKubernetesProcessor> {}".format(self.name)


def notebook_job_output(result: JobDict) -> Tuple[Optional[str], Any]:

    # NOTE: this assumes that we have user home under the same path as jupyter
    notebook_path = Path(result["result-notebook"])

    _wait_for_result_file(notebook_path)
    scraps = scrapbook.read_notebook(str(notebook_path)).scraps

    LOGGER.debug("Retrieved scraps from notebook: %s", scraps)

    if not scraps:
        return (None, {"result-link": result["result-link"]})
    elif (result_file_scrap := scraps.get("result-file")) :  # noqa
        # if available, prefer file output
        specified_path = Path(result_file_scrap.data)
        result_file_path = (
            specified_path
            if specified_path.is_absolute()
            else CONTAINER_HOME / specified_path
        )
        # NOTE: use python-magic or something more advanced if necessary
        mime_type = mimetypes.guess_type(result_file_path)[0]
        return (mime_type, result_file_path.read_bytes())
    elif len(scraps) == 1:
        # if there's only one item, return it right away with correct content type.
        # this way, you can show e.g. an image in the browser.
        # otherwise, we just return the scrap structure
        return serialize_single_scrap(next(iter(scraps.values())))
    else:
        # TODO: support serializing multiple scraps, possibly according to result schema:
        # https://github.com/opengeospatial/wps-rest-binding/blob/master/core/openapi/schemas/result.yaml
        return serialize_single_scrap(next(iter(scraps.values())))


def _wait_for_result_file(notebook_path: Path) -> None:
    # If the result file is queried immediately after the job has finished, it's likely
    # that the s3fs mount has not yet received the changes that were written by the job.
    # So instead of failing right away here, we detect the situation and wait.
    for _ in range(20):
        # NOTE: s3fs will only be refreshed on operations such as these. However
        #       the refresh takes some time, which is ok here because we will catch it
        #       in a subsequent loop run
        notebook_path.open().close()
        if notebook_path.stat().st_size != 0:
            LOGGER.info("Result file present")
            break
        else:
            LOGGER.info("Waiting for result file")
            time.sleep(1)


def serialize_single_scrap(scrap: scrapbook.scraps.Scrap) -> Tuple[Optional[str], Any]:

    text_mime = "text/plain"

    if scrap.display:
        # we're only interested in display_data
        # https://ipython.org/ipython-doc/dev/notebook/nbformat.html#display-data

        if scrap.display["output_type"] == "display_data":
            # data contains representations with different mime types as keys. we
            # want to prefer non-text
            mime_type = next(
                (f for f in scrap.display["data"] if f != text_mime),
                text_mime,
            )
            item = scrap.display["data"][mime_type]
            encoded_output = item if mime_type == text_mime else b64decode(item)
            return (mime_type, encoded_output)
        else:
            return None, scrap.display
    else:
        return None, scrap.data


def default_output_path(notebook_path: str) -> str:
    filename_without_postfix = re.sub(".ipynb$", "", notebook_path)
    now_formatted = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    return filename_without_postfix + f"_result_{now_formatted}.ipynb"


def setup_output_notebook(
    output_directory: Path,
    output_notebook_filename: str,
) -> Path:
    output_notebook = output_directory / output_notebook_filename

    # create output directory owned by root (readable, but not changeable by user)
    output_directory.mkdir(exist_ok=True, parents=True)

    # create file owned by root but group is job runner group.
    # this way we can execute the job with the jovyan user with the additional group,
    # which the actual user (as in human) in jupyterlab does not have.
    output_notebook.touch(exist_ok=False)
    # TODO: reasonable error when output notebook already exists
    os.chown(output_notebook, uid=0, gid=JOB_RUNNER_GROUP_ID)
    os.chmod(output_notebook, mode=0o664)
    return output_notebook


def working_dir(notebook_path: PurePath) -> PurePath:
    abs_notebook_path = (
        notebook_path
        if notebook_path.is_absolute()
        else (CONTAINER_HOME / notebook_path)
    )
    return abs_notebook_path.parent


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


def gpu_extra_podspec() -> Dict:
    node_selector = k8s_client.V1NodeSelector(
        node_selector_terms=[
            k8s_client.V1NodeSelectorTerm(
                match_expressions=[
                    k8s_client.V1NodeSelectorRequirement(
                        key="hub.eox.at/node-purpose",
                        operator="In",
                        values=["g2"],
                    ),
                ]
            )
        ]
    )
    return {
        "affinity": k8s_client.V1Affinity(
            node_affinity=k8s_client.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=node_selector
            )
        ),
        "tolerations": [
            k8s_client.V1Toleration(
                key="hub.eox.at/gpu", operator="Exists", effect="NoSchedule"
            )
        ],
    }


def home_volume_config(home_volume_claim_name: str) -> ExtraConfig:
    return ExtraConfig(
        volumes=[
            k8s_client.V1Volume(
                persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=home_volume_claim_name,
                ),
                name="home",
            )
        ],
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=str(CONTAINER_HOME),
                name="home",
            )
        ],
    )


def extra_pvc_config(extra_pvc: Dict) -> ExtraConfig:
    extra_name = f"extra-{extra_pvc['num']}"
    return ExtraConfig(
        volumes=[
            k8s_client.V1Volume(
                persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=extra_pvc["claim_name"],
                ),
                name=extra_name,
            )
        ],
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=extra_pvc["mount_path"],
                name=extra_name,
            )
        ],
    )


def extra_secret_config(secret_name: str, num: int) -> ExtraConfig:
    volume_name = f"secret-{num}"
    return ExtraConfig(
        volumes=[
            k8s_client.V1Volume(
                secret=k8s_client.V1SecretVolumeSource(secret_name=secret_name),
                name=volume_name,
            )
        ],
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=str(PurePath("/secret") / secret_name),
                name=volume_name,
            )
        ],
    )


def git_checkout_config(
    url: str, secret_name: str, git_revision: Optional[str]
) -> ExtraConfig:
    git_sync_mount_name = "git-sync-mount"

    init_container = k8s_client.V1Container(
        name="git-sync",
        image="k8s.gcr.io/git-sync:v3.1.6",
        volume_mounts=[
            k8s_client.V1VolumeMount(
                name=git_sync_mount_name,
                mount_path="/tmp/git",  # as per container default
            ),
        ],
        env=[
            k8s_client.V1EnvVar(
                name="GIT_SYNC_REPO",
                value=url,
            ),
            k8s_client.V1EnvVar(name="GIT_SYNC_DEST", value=GIT_CHECKOUT_PATH.name),
            k8s_client.V1EnvVar(
                name="GIT_SYNC_ONE_TIME",
                value="true",
            ),
            k8s_client.V1EnvVar(
                name="GIT_SYNC_USERNAME",
                value_from=k8s_client.V1EnvVarSource(
                    secret_key_ref=k8s_client.V1SecretKeySelector(
                        name=secret_name,
                        key="username",
                    ),
                ),
            ),
            k8s_client.V1EnvVar(
                name="GIT_SYNC_PASSWORD",
                value_from=k8s_client.V1EnvVarSource(
                    secret_key_ref=k8s_client.V1SecretKeySelector(
                        name=secret_name,
                        key="password",
                    ),
                ),
            ),
        ]
        + (
            [k8s_client.V1EnvVar(name="GIT_SYNC_REV", value=git_revision)]
            if git_revision
            else []
        ),
    )

    return ExtraConfig(
        init_containers=[init_container],
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=str(GIT_CHECKOUT_PATH.parent),
                name=git_sync_mount_name,
            )
        ],
        volumes=[
            k8s_client.V1Volume(
                name=git_sync_mount_name,
                empty_dir=k8s_client.V1EmptyDirVolumeSource(),
            )
        ],
    )


def s3_config(bucket_name, secret_name, s3_url) -> ExtraConfig:
    s3_user_bucket_volume_name = "s3-user-bucket"
    return ExtraConfig(
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=str(S3_MOUNT_PATH),
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
                image="totycro/s3fs:0.6.0-1.86",
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
                    limits={"cpu": "0.1", "memory": "128Mi"},
                    requests={
                        "cpu": "0.05",
                        "memory": "32Mi",
                    },
                ),
                env=[
                    k8s_client.V1EnvVar(name="S3FS_ARGS", value="-oallow_other"),
                    k8s_client.V1EnvVar(name="UID", value=S3_MOUNT_UID),
                    k8s_client.V1EnvVar(name="GID", value=S3_MOUNT_GID),
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


def s3_subdir_cmd(subdir: str, job_name: str):
    subdir_expanded = subdir.format(job_name=job_name)
    # make sure this is only a path, not something really malicious
    subdir_validated = PurePath(subdir_expanded).name
    path_to_subdir = S3_MOUNT_PATH / subdir_validated
    return (
        f'mkdir "{path_to_subdir}" &&  '
        f'ln -sf --no-dereference "{path_to_subdir}" "{RESULT_DATA_PATH}" && '
        # NOTE: no-dereference is useful if home is a persisted mounted volume
    )


def drop_none_values(d: Dict) -> Dict:
    return {k: v for k, v in d.items() if v is not None}
