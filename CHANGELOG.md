## 1.1.8
* Add new `extra_volumes` and `extra_volume_mounts` config

## 1.1.7
* Remove result-link feature

## 1.1.6
* Remove unused job_resources view

## 1.1.5
* Use more recent pygeoapi version
* Improve error handling on missing job id

## 1.1.4
* Add `output_dirname` parameter

## 1.1.0, 1.1.1, 1.1.2, 1.1.3
* Update to pygeoapi 0.15.0

## 1.0.23
* Add support for conda store kernels

## 1.0.19
* Add results_in_output_dir config option

## 1.0.17
* Version jump due to accidental chart misversioning
* Remove custom job setup script feature

## 1.0.14
* Set HOME env var to support nebari based images

## 1.0.13
* Use jovyan user as owner of result notebook to try to avoid spurious permission denied

## 1.0.12
* Add more qhub-related options
* Run papermill_slack also if papermill fails

## 1.0.10
* Run papermill_slack if env var is defined

## 1.0.9
* Setup for new hub

## 1.0.6
* Save parameters which are passed as yaml as json

## 1.0.5
* Fix build

## 1.0.4
* Allow dynamic override of node_purpose protected by allowed_node_purposes_regex

## 1.0.3
* Delete pods of cancelled jobs by force

## 1.0.2
* Write output files in subdirectory according to the current day
* Disable papermill autosave

## 1.0.1
* Disable autosave in papermill
* Write output files in subdirectory according to the current day

## 1.0.1
* Update s3fs
* Obfusicate parameters which look like passwords in UI

## 1.0.0
* This is really the same as 0.0.78 but it feels stable now

## 0.0.78
* Support arbitrary default branch name on git checkout

## 0.0.77
* Allow specifying images for each execution

## 0.0.76
* Delete pods of jobs when deleting job

## 0.0.75
* Fixed label setting

## 0.0.74
* Add job id to default job output filename
* Implement automatically mounting EDC secrets
* Disable service links

## 0.0.73
* Allow passing custom tolerations

## 0.0.72
* Allow specifying sub_path for extra pvcs

## 0.0.71
* Implement run_on_fargate as request param and allow_fargate in config

## 0.0.70
* Show more infos for accepted jobs (e.g. not runnig because resource requests too high)

## 0.0.69
* Allow job service account to be configured

## 0.0.67
* Allow configuring node_purpose

## 0.0.66
* Add log_output config

## 0.0.65
* Extend gpu image detection to support aws images

## 0.0.64
* Fix retrieving resource usage of jobs with long names

## 0.0.63
* Hotfix

## 0.0.62
* git checkout as jovyan user

## 0.0.61
* Generalize result data directories to arbitrary dirs, not just s3

## 0.0.60
* Allow mounting secrets as env vars via `env_from`

## 0.0.58
* Wait for result file if queried right after job finish
* Make sidecar container termination more robust 
* Add patch to allow overwriting job ids
* Allow specifying git revisions in job request parameters

## 0.0.54
* Implement resources endpoint for jobs

## 0.0.53
* Update to latest pygeoapi api (mime type for sync jobs)

## 0.0.52
* Implement `result_data_directory` parameter for jobs (this mounts a subdir of s3 in `/home/jovyan/result-data`)
* Add option for git checkout as init container

## 0.0.51
* Allow for secrets for jobs

## 0.0.50
* Handle kubernetes interface empty lists being null

## 0.0.49
* Set propagation policy when deleting kubernetes jobs

## 0.0.47, 0.0.48
* Fix build issues

## 0.0.46
* Don't wait for s3 forever
* Fix crash when no extra config is specified
* Add kernel option to wps-client

## 0.0.45
* Validate job parameter

## 0.0.44
* Set async as default again

## 0.0.43
* Add wps-client.py

## 0.0.43
* Switch to new repo and package

## 0.0.42
* Improve s3 mount start detection

## 0.0.41
* Wait for s3 mount before starting job
* Change permission error workaround

## 0.0.40
* Reduced job web-UI due to changes upstream
* Use new job result logic from upstream

## 0.0.39
* Use pygeoapi/master as base since the manager branch has been merged
* Web UI is now reduced (can't start jobs anymore in browser)

