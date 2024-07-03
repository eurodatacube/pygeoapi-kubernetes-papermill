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

from collections.abc import Callable
from typing import Optional
import copy

import pytest

from pygeoapi_kubernetes_papermill.image import (
    ContainerImageKubernetesProcessor,
)


@pytest.fixture()
def create_processor() -> Callable[[Optional[dict]], ContainerImageKubernetesProcessor]:
    def _create_processor(def_override=None):
        return ContainerImageKubernetesProcessor(
            processor_def={
                "name": "test",
                "default_image": "example-image",
                "command": "",
                "allowed_images_regex": "",
                "s3": None,
                "extra_volumes": [],
                "extra_volume_mounts": [],
                "node_purpose_label_key": "",
                "default_node_purpose": "",
                "image_pull_secret": "",
                "allowed_node_purposes_regex": "",
                "tolerations": [],
                "allow_fargate": False,
                "parameters_env": {},
                "secrets": [],
                **(def_override if def_override else {}),
            }
        )

    return _create_processor


@pytest.fixture()
def create_pod_kwargs() -> dict:
    return {
        "data": {"parameters_env": {}},
        "job_name": "my-job",
    }


@pytest.fixture()
def create_pod_kwargs_with(create_pod_kwargs) -> Callable:
    def create(data):
        kwargs = copy.deepcopy(create_pod_kwargs)
        kwargs["data"].update(data)
        return kwargs

    return create


def test_basic_pod_spec_is_generated(create_processor, create_pod_kwargs):
    spec = create_processor().create_job_pod_spec(**create_pod_kwargs)
    assert spec.pod_spec.containers[0].image == "example-image"


def test_env_is_combined_from_conf_and_req(create_processor, create_pod_kwargs_with):
    spec = create_processor(
        {"parameters_env": {"from_conf": "there"}}
    ).create_job_pod_spec(
        **create_pod_kwargs_with(
            {
                "parameters_env": {"from_request": "here"},
            }
        )
    )

    assert {var.name: var.value for var in spec.pod_spec.containers[0].env} == {
        "from_request": "here",
        "from_conf": "there",
    }


def test_command_is_combined_with_results_dir_setup(
    create_processor, create_pod_kwargs_with
):
    spec = create_processor({"command": "true"}).create_job_pod_spec(
        **create_pod_kwargs_with({"result_data_directory": "my-run-r0000"})
    )

    command = spec.pod_spec.containers[0].command
    assert command[0] == "bash"
    assert command[3].endswith("&& true")
    assert 'mkdir "/full-results-pvc/my-run-r0000";' in command[3]


def test_env_parameters_are_saved_as_json(create_processor, create_pod_kwargs_with):
    spec = create_processor().create_job_pod_spec(
        **create_pod_kwargs_with({"parameters_env": {"a": "b"}})
    )
    assert spec.extra_annotations["parameters"] == '{"a": "b"}'


def test_secrets_can_be_injected_via_env_vars(create_processor, create_pod_kwargs):
    spec = create_processor(
        {"secrets": [{"name": "secA", "access": "env"}]}
    ).create_job_pod_spec(**create_pod_kwargs)

    assert "secA" == spec.pod_spec.containers[0].env_from[0].secret_ref.name


def test_generic_process_metadata_is_used_from_config(create_processor):
    proc = create_processor(
        {
            "id": "m1606",
            "title": "my-proc1606",
            "version": "0.0.86",
            "inputs": [{"minOccurs": 3}],
        }
    )
    assert proc.metadata["id"] == "m1606"
    assert proc.metadata["title"] == "my-proc1606"
    assert proc.metadata["version"] == "0.0.86"
    assert proc.metadata["inputs"][0]["minOccurs"] == 3
