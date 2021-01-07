import click
import re
import yaml
import requests


@click.command()
@click.argument('notebook', type=click.Path(exists=True))
@click.argument('output_path', type=click.Path(exists=False))
@click.argument('parameters_path', type=click.Path(exists=True),)
@click.argument('wps_endpoint', type=click.STRING)
@click.option('--cpu', default=None, help='number of CPUs format: request/limit')
@click.option('--mem', default=None, help='memory in GB format: request/limit')
def main(notebook, output_path, parameters_path, wps_endpoint, cpu, mem):
    def parse_limit_reqest(param):

        re_float = "(\d+(?:\.\d*)?)"
        if not param:
            return "", ""
        split = re.findall(r"^{re_float}/{re_float}$".format(re_float=re_float), param)[0]
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

        return dict(
            id=key,
            value=value
        )

    cpu_request, cpu_limit = parse_cpu(cpu)
    mem_request, mem_limit = parse_mem(mem)

    with open(parameters_path) as f:
        parameters = yaml.load(f, Loader=yaml.FullLoader)

    parameters["_execution"] = dict(
        notebook=notebook,
        output_path=output_path,
        parameters_path=parameters_path,
        cpu_request=cpu_request,
        cpu_limit=cpu_limit,
        mem_request=mem_request,
        mem_limit=mem_limit
    )

    headers = {
        'Content-type': 'application/json',
    }

    url = 'https://{wps_endpoint}/processes/execute-notebook/jobs?async-execute=True'.format(wps_endpoint=wps_endpoint)

    inputs = [
        parameter("notebook", notebook),
        parameter("output_path", output_path),
        parameter("parameters_json", parameters),

    ]

    if cpu:
        inputs.append(parameter("cpu_request", cpu_request))
        inputs.append(parameter("cpu_limit", cpu_limit))

    if mem:
        inputs.append(parameter("mem_request", mem_request))
        inputs.append(parameter("mem_limit", mem_limit))

    response = requests.post(url, headers=headers, data=str({"inputs": inputs}).replace("\'", "\""))
    print(response)


if __name__ == "__main__":
    main()
