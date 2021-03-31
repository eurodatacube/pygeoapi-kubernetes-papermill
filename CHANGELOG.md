## [Unreleased]
* Implement `result_data_directory` parameter for jobs (this mounts a subdir of s3 in `/home/jovyan/result-data`)

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

