# pygeoapi-kubernetes-papermill

Extends pygeoapi by a manager for kubernetes jobs and a process to execute notebooks via papermill on a cluster.

For each pygeoapi job, a kubernetes job is spawned which runs papermill in a docker image.
You can use the default [eurodatacube base image](https://hub.docker.com/repository/docker/eurodatacube/jupyter-user/) or configure your own image.

Jobs can be started with different parameters. Note that the path to the notebook file itself is a parameter, so by default, you can execute any notebook available to the job container.

A helm chart is available at https://github.com/eurodatacube/charts/tree/master/pygeoapi-eoxhub.

## Installation

### Docker
You can use the [Dockerfile](Dockerfile) to get started. It's based on `geopython/pygeoapi:latest` and installs `pygeoapi-kubernetes-papermill` directly via `setup.py`.

### Other environments
1. [Install pygeoapi](https://docs.pygeoapi.io/en/latest/installation.html).
1. Install `pygeoapi-kubernetes-papermill` directly from `git` via `pip`:  
  `python3 -m pip install git+git://github.com/eurodatacube/pygeoapi-kubernetes-papermill.git`

Proper packages may be provided in the future.

## Submitting and monitoring jobs

Please consult the `eurodatacube` user documentation:
https://eurodatacube.com/documentation/headless-notebook-execution

## Kubernetes cluster setup

To really make this useful, you are going to need to think about how to integrate the job workflow to your existing environment.

A common case is to allow users to edit end debug their notebooks in a kubernetes-hosted JupyterLab and allow the jobs full read and write access to the user home in JupyterLab.

[The helm chart used by `eurodatacube`](https://github.com/eurodatacube/charts/tree/master/pygeoapi-eoxhub) provides an example of the required kubernetes configuration. It contains:
* A deployment of pygeoapi including service, ingress
* Permissions for the deployment to start and list jobs
* A pygeoapi config file

###  Pygeoapi config


In order to activate `pygeoapi-kubernetes-papermill`, you will need to set up the `kubernetes` manager and at least one notebook processing job.
A helm-templated complete example can be [found here](https://github.com/eurodatacube/charts/blob/master/pygeoapi-eoxhub/templates/config.yaml).

The manager has no configuration options:
```yaml
manager:
    name: pygeoapi_kubernetes_papermill.KubernetesManager
```

The process can (and probably should) be configured (see below):
```yaml
execute-notebook:
  type: process
  processor:
    name: pygeoapi_kubernetes_papermill.PapermillNotebookKubernetesProcessor
    default_image: "docker/example-image:1-2-3"
    image_pull_secret: "my-pull-secret"
    s3:
      bucket_name: "my-s3-bucket"
      secret_name: "secret-for-my-s3-bucket"
      s3_url: "https://s3-eu-central-1.amazonaws.com"
    output_directory: "/home/jovian/my-jobs"
    home_volume_claim_name: "user-foo"
    extra_pvcs:
      - claim_name: more-data
        mount_path: /home/jovyan/more-data
    jupyter_base_url: "https://example.com/jupyter"
    secrets:
      - name: "s3-access-credentials"  # defaults to access via mount
      - name: "db"
        access: "mount"
      - name: "redis"
        access: "env"
    checkout_git_repo:
      url: https://gitlab.example.com/repo.git
      secret_name: pygeoapi-git-secret
    log_output: false
```


`default_image`:
Image to be used to execute the job.
It needs to contain `papermill` in `$PATH`.
You can use [this papermill engine](https://github.com/eurodatacube/papermill-kubernetes-job-progress) to notify kubernetes about the job progress.

`image_pull_secret:`
(Optional): Pull secret for the docker image.

`s3` (Optional):
Activate [this s3fs sidecar container](https://github.com/totycro/docker-s3fs-client) to make an s3 bucket available in the filesystem of the job so you can directly read from and write to it.

`output_directory`: Output directory for jobs in the docker container.

`home_volume_claim_name` (Optional):
Persistent volume claim name of user home.
This volume claim will be made available under `/home/jovyan` to the running job.

`extra_pvcs` (Optional):
List of other volume claims that are available to the job.

`jupyter_base_url` (Optional):
If this is specified, then a link to result notebooks in jupyterlab is generated.
Note that the user home must be the same in JupyterLab and the job container for this to work.


Note that you can arbitrarily combine `s3`, `home_volume_claim_name` and `extra_pvcs`. This means that it's possible to e.g. only use `s3` to fetch notebooks and store results, or to use only other volume claims, or any combination of these.

`secrets` (Optional):
List of secrets which will be mounted as volume under `/secret/<secret-name>` or made available as environment variable.

`checkout_git_repo` (Optional):
Clone a git repo to /home/jovyan/git/algorithm before the job starts. Useful to execute the latest version of notebooks or code of that repository. `secret_name` must contain `username` and `password` for git https checkout.

`log_output`:
Boolean, whether to enable `--log-output` in papermill.
