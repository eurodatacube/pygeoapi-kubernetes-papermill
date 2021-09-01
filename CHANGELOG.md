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

