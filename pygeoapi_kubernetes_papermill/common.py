_JOB_NAME_PREFIX = "pygeoapi-job-"


def k8s_job_name(job_id: str) -> str:
    return f"{_JOB_NAME_PREFIX}{job_id}"


def is_k8s_job_name(job_name: str) -> bool:
    return job_name.startswith(_JOB_NAME_PREFIX)


def job_id_from_job_name(job_name: str) -> str:
    return job_name[len(_JOB_NAME_PREFIX):]
