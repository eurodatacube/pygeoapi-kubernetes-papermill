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

from base64 import b64encode
import copy
import json
from pathlib import Path
from pygeoapi.process.base import ProcessorExecuteError
import pytest
import stat
from typing import Dict, Callable


from pygeoapi_kubernetes_papermill.kubernetes import JobDict
from pygeoapi_kubernetes_papermill.notebook import (
    CONTAINER_HOME,
    JOB_RUNNER_GROUP_ID,
    PapermillNotebookKubernetesProcessor,
    notebook_job_output,
)

OUTPUT_DIRECTORY = "/home/jovyan/foo/test"


@pytest.fixture(autouse=True)
def cleanup_job_directory():
    if (job_dir := Path(OUTPUT_DIRECTORY)).exists():
        for file in job_dir.iterdir():
            file.unlink()


def _create_processor(def_override=None) -> PapermillNotebookKubernetesProcessor:
    return PapermillNotebookKubernetesProcessor(
        processor_def={
            "name": "test",
            "s3": None,
            "default_image": "example",
            "extra_pvcs": [],
            "home_volume_claim_name": "user",
            "image_pull_secret": "",
            "jupyter_base_url": "",
            "output_directory": OUTPUT_DIRECTORY,
            "secrets": [],
            "log_output": False,
            **(def_override if def_override else {}),
        }
    )


@pytest.fixture()
def papermill_processor() -> PapermillNotebookKubernetesProcessor:
    return _create_processor()


@pytest.fixture()
def papermill_gpu_processor() -> PapermillNotebookKubernetesProcessor:
    return _create_processor({"default_image": "jupyter-user-g:1.2.3"})


@pytest.fixture()
def create_pod_kwargs() -> Dict:
    return {
        "data": {"notebook": "a", "parameters": ""},
        "job_name": "my-job",
    }


@pytest.fixture()
def create_pod_kwargs_with(create_pod_kwargs) -> Callable:
    def create(data):
        kwargs = copy.deepcopy(create_pod_kwargs)
        kwargs["data"].update(data)
        return kwargs

    return create


def test_workdir_is_notebook_dir(papermill_processor):
    relative_dir = "a/b"
    nb_path = f"{relative_dir}/a.ipynb"
    abs_dir = f"/home/jovyan/{relative_dir}"

    job_pod_spec = papermill_processor.create_job_pod_spec(
        data={"notebook": nb_path, "parameters": ""},
        job_name="",
    )

    assert f'--cwd "{abs_dir}"' in str(job_pod_spec.pod_spec.containers[0].command)


def test_json_params_are_b64_encoded(papermill_processor, create_pod_kwargs_with):
    payload = {"a": 3}
    job_pod_spec = papermill_processor.create_job_pod_spec(
        **create_pod_kwargs_with({"parameters_json": payload})
    )

    assert b64encode(json.dumps(payload).encode()).decode() in str(
        job_pod_spec.pod_spec.containers[0].command
    )


def test_custom_output_file_overwrites_default(
    papermill_processor, create_pod_kwargs_with
):
    output_path = "foo/bar.ipynb"
    job_pod_spec = papermill_processor.create_job_pod_spec(
        **create_pod_kwargs_with({"output_filename": output_path})
    )

    assert "bar.ipynb" in str(job_pod_spec.pod_spec.containers[0].command)


def test_output_is_written_to_output_dir(create_pod_kwargs):
    output_dir = "/home/jovyan"

    processor = _create_processor({"output_directory": output_dir})
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)

    assert output_dir + "/a_result" in str(job_pod_spec.pod_spec.containers[0].command)


def test_gpu_image_produces_gpu_kernel(papermill_gpu_processor, create_pod_kwargs):
    job_pod_spec = papermill_gpu_processor.create_job_pod_spec(**create_pod_kwargs)
    assert "-k edc-gpu" in str(job_pod_spec.pod_spec.containers[0].command)


def test_default_image_has_no_affinity(papermill_processor, create_pod_kwargs):
    job_pod_spec = papermill_processor.create_job_pod_spec(**create_pod_kwargs)

    assert job_pod_spec.pod_spec.affinity is None
    assert job_pod_spec.pod_spec.tolerations is None


def test_gpu_image_has_affinity(papermill_gpu_processor, create_pod_kwargs):
    job_pod_spec = papermill_gpu_processor.create_job_pod_spec(**create_pod_kwargs)

    node_affinity = job_pod_spec.pod_spec.affinity.node_affinity
    r = node_affinity.required_during_scheduling_ignored_during_execution
    assert r.node_selector_terms[0].match_expressions[0].values == ["g2"]
    assert job_pod_spec.pod_spec.tolerations[0].key == "hub.eox.at/gpu"


def test_no_s3_bucket_by_default(papermill_processor, create_pod_kwargs):
    job_pod_spec = papermill_processor.create_job_pod_spec(**create_pod_kwargs)
    assert "s3mounter" not in [c.name for c in job_pod_spec.pod_spec.containers]
    assert "/home/jovyan/s3" not in [
        m.mount_path for m in job_pod_spec.pod_spec.containers[0].volume_mounts
    ]
    assert "wait for s3" not in str(job_pod_spec.pod_spec.containers[0].command)


@pytest.fixture()
def papermill_processor_s3():
    return _create_processor(
        {"s3": {"bucket_name": "example", "secret_name": "example", "s3_url": ""}}
    )


def test_s3_bucket_present_when_requested(papermill_processor_s3, create_pod_kwargs):
    job_pod_spec = papermill_processor_s3.create_job_pod_spec(**create_pod_kwargs)
    assert "s3mounter" in [c.name for c in job_pod_spec.pod_spec.containers]
    assert "/home/jovyan/s3" in [
        m.mount_path for m in job_pod_spec.pod_spec.containers[0].volume_mounts
    ]
    assert "wait for s3" in str(job_pod_spec.pod_spec.containers[0].command)


def test_job_specific_s3_subdir_is_mounted(
    papermill_processor_s3, create_pod_kwargs_with
):
    job_pod_spec = papermill_processor_s3.create_job_pod_spec(
        **create_pod_kwargs_with(
            {"result_data_directory": "s3/foo-{job_name}"},
        )
    )

    cmd = str(job_pod_spec.pod_spec.containers[0].command)
    assert 'mkdir "/home/jovyan/s3/foo-my-job"' in cmd
    assert (
        'ln -sf --no-dereference "/home/jovyan/s3/foo-my-job" "/home/jovyan/result-data'
        in cmd
    )


def test_extra_pvcs_are_added_on_request(create_pod_kwargs):
    claim_name = "my_pvc"
    processor = _create_processor(
        {"extra_pvcs": [{"claim_name": claim_name, "mount_path": "/mnt"}]}
    )
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)

    assert claim_name in [
        v.persistent_volume_claim.claim_name
        for v in job_pod_spec.pod_spec.volumes
        if v.persistent_volume_claim
    ]


def test_image_pull_secr_added_when_requested(create_pod_kwargs):
    processor = _create_processor({"image_pull_secret": "psrcr"})
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)
    assert job_pod_spec.pod_spec.image_pull_secrets[0].name == "psrcr"


def test_output_path_owned_by_job_runner_group_and_group_writable(
    papermill_processor, create_pod_kwargs_with
):
    output_filename = "foo.ipynb"
    papermill_processor.create_job_pod_spec(
        **create_pod_kwargs_with({"output_filename": output_filename})
    )

    output_notebook = Path(OUTPUT_DIRECTORY) / output_filename
    assert output_notebook.stat().st_gid == JOB_RUNNER_GROUP_ID

    assert output_notebook.stat().st_mode & stat.S_IWGRP  # write group


def test_custom_kernel_is_used_on_request(papermill_processor, create_pod_kwargs_with):
    my_kernel = "my-kernel"
    job_pod_spec = papermill_processor.create_job_pod_spec(
        **create_pod_kwargs_with({"kernel": my_kernel})
    )

    assert f"-k {my_kernel}" in str(job_pod_spec.pod_spec.containers[0].command)


def test_no_kernel_specified_if_not_detected(papermill_processor, create_pod_kwargs):
    job_pod_spec = papermill_processor.create_job_pod_spec(**create_pod_kwargs)

    assert "-k " not in str(job_pod_spec.pod_spec.containers[0].command)


def test_error_for_invalid_parameter_is_raised(
    papermill_processor, create_pod_kwargs_with
):
    with pytest.raises(ProcessorExecuteError):
        papermill_processor.create_job_pod_spec(
            **create_pod_kwargs_with({"not_a_valid_parameter": 3})
        )


@pytest.fixture
def generate_scrap_notebook(tmp_path):
    def gen(output_name: str, data) -> Path:
        nb_data = {
            "cells": [
                {
                    "cell_type": "code",
                    "metadata": {},
                    "execution_count": 1,
                    "outputs": [
                        {
                            "data": {
                                "application/scrapbook.scrap.text+json": {
                                    "data": data,
                                    "encoder": "text",
                                    "name": output_name,
                                    "version": 1,
                                }
                            },
                            "metadata": {
                                "scrapbook": {
                                    "data": True,
                                    "display": False,
                                    "name": "result-file",
                                }
                            },
                            "output_type": "display_data",
                        }
                    ],
                    "source": ["some code\n"],
                },
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }
        nb_filepath = tmp_path / "a.ipynb"
        json.dump(nb_data, open(nb_filepath, "w"))
        return nb_filepath

    return gen


@pytest.fixture
def job_dict() -> JobDict:
    return {
        "status": "successful",
        "result-link": "https://example.com",
        "result-notebook": "a.ipynb",
    }


def test_notebook_output_returns_a_text_scrap(generate_scrap_notebook, job_dict):
    payload = "my_payload"
    nb_filepath = generate_scrap_notebook(output_name="my_output", data=payload)
    job_dict["result-notebook"] = str(nb_filepath)

    output = notebook_job_output(job_dict)

    assert output == (None, payload)


def test_notebook_output_resolves_files_from_scrap(generate_scrap_notebook, job_dict):
    filename = "a.tif"

    tif_payload = b"II*\x00\x08\x00\x00\x00\x10\x00\x00\x01\x03\x00\x01\x00"
    nb_filepath = generate_scrap_notebook(output_name="result-file", data=filename)
    job_dict["result-notebook"] = str(nb_filepath)
    (CONTAINER_HOME / filename).write_bytes(tif_payload)

    output = notebook_job_output(job_dict)

    (CONTAINER_HOME / filename).unlink()

    assert output == ("image/tiff", tif_payload)


def test_secrets_are_being_mounted_by_default(create_pod_kwargs):
    processor = _create_processor({"secrets": [{"name": "secA"}]})
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)

    assert "secA" in [
        v_sec.secret_name
        for vol in job_pod_spec.pod_spec.volumes
        if (v_sec := vol.secret)
    ]
    assert "/secret/secA" in [
        m.mount_path for m in job_pod_spec.pod_spec.containers[0].volume_mounts
    ]


def test_secrets_can_be_injected_via_env_vars(create_pod_kwargs):
    processor = _create_processor({"secrets": [{"name": "secA", "access": "env"}]})
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)
    assert "secA" == job_pod_spec.pod_spec.containers[0].env_from[0].secret_ref.name


def test_git_checkout_init_container_is_added(create_pod_kwargs):
    checkout_conf = {
        "checkout_git_repo": {
            "url": "https://gitlab.example.at/example.git",
            "secret_name": "pygeoapi-git-secret",
        }
    }
    processor = _create_processor(checkout_conf)

    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)
    assert "git-sync" in [c.name for c in job_pod_spec.pod_spec.init_containers]
    assert "/home/jovyan/git" in [
        m.mount_path for m in job_pod_spec.pod_spec.containers[0].volume_mounts
    ]


def test_git_revision_can_be_set_via_request(create_pod_kwargs_with):
    checkout_conf = {
        "checkout_git_repo": {
            "url": "https://gitlab.example.at/example.git",
            "secret_name": "pygeoapi-git-secret",
        }
    }
    processor = _create_processor(checkout_conf)

    git_revision = "abc"
    job_pod_spec = processor.create_job_pod_spec(
        **create_pod_kwargs_with({"git_revision": git_revision})
    )

    assert any(
        env.name == "GIT_SYNC_REV" and env.value == git_revision
        for init_container in job_pod_spec.pod_spec.init_containers
        for env in init_container.env
    )


def test_log_output_is_activated_on_demand(create_pod_kwargs):
    processor = _create_processor({"log_output": True})
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)

    assert "--log-output " in str(job_pod_spec.pod_spec.containers[0].command)
