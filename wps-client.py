import click
import re
import requests
import pprint
import json


@click.command()
@click.argument("wps_endpoint", type=click.STRING)
@click.argument("notebook", type=click.Path())
@click.option("--parameters", default=None, type=json.loads)
@click.option("--output_filename", default=None, type=click.Path())
@click.option("--cpu", default=None, help="number of CPUs format: request/limit")
@click.option("--mem", default=None, help="memory in GB format: request/limit")
@click.option("--kernel", default=None)
@click.option("--result_data_directory", default=None)
def main(
    notebook,
    output_filename,
    parameters,
    wps_endpoint,
    cpu,
    mem,
    kernel,
    result_data_directory,
):
    parameters = parameters or {}

    def parse_limit_reqest(param):
        re_float = r"(\d+(?:\.\d*)?)"
        if not param:
            return "", ""
        split = re.findall(
            r"^{re_float}/{re_float}$".format(re_float=re_float),
            param,
        )[0]
        return float(split[0]), float(split[1])

    def parse_mem(value):
        if not value:
            return "", ""

        limit, request = parse_limit_reqest(value)
        return str(limit) + "Gi", str(request) + "Gi"

    def parse_cpu(value):
        limit, request = parse_limit_reqest(value)
        return str(limit), str(request)

    def parameter(key, value):
        return dict(id=key, value=value)

    cpu_requests, cpu_limit = parse_cpu(cpu)
    mem_requests, mem_limit = parse_mem(mem)

    # trick to "record" execution parameters
    parameters["_execution"] = dict(
        notebook=notebook,
        output_filename=output_filename,
        cpu_requests=cpu_requests,
        cpu_limit=cpu_limit,
        mem_requests=mem_requests,
        mem_limit=mem_limit,
        kernel=kernel,
        result_data_directory=result_data_directory,
    )

    inputs = [
        parameter("notebook", notebook),
        parameter("parameters_json", parameters),
    ] + ([parameter("output_filename", output_filename)] if output_filename else [])

    if cpu:
        inputs.append(parameter("cpu_requests", cpu_requests))
        inputs.append(parameter("cpu_limit", cpu_limit))

    if mem:
        inputs.append(parameter("mem_requests", mem_requests))
        inputs.append(parameter("mem_limit", mem_limit))

    if kernel:
        inputs.append(parameter("kernel", kernel))

    if result_data_directory:
        inputs.append(parameter("result_data_directory", result_data_directory))

    print("Sending request:")
    pprint.pprint(inputs)
    response = requests.post(
        f"https://{wps_endpoint}/processes/execute-notebook/jobs",
        headers={
            "Content-type": "application/json",
        },
        json={"inputs": inputs},
        verify=False,
    )
    print(response)
    print(response.content.decode())
    print(f"Link to job: {response.headers['Location']}")


if __name__ == "__main__":
    main()
