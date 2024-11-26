test: build
	docker run --env PYGEOAPI_CONFIG=/pkp/tests/pygeoapi-test-config.yaml --env PYGEOAPI_OPENAPI=/pygeoapi/tests/pygeoapi-test-openapi.yml --entrypoint python3 pkp:1 -m pytest
	docker run --entrypoint flake8 pkp:1 pygeoapi_kubernetes_papermill tests
	docker run --entrypoint mypy pkp:1 pygeoapi_kubernetes_papermill tests

build:
	docker build . -t pkp:1

bash: build
	docker run --env PYGEOAPI_CONFIG=/pkp/tests/pygeoapi-test-config.yaml --env PYGEOAPI_OPENAPI=/pygeoapi/tests/pygeoapi-test-openapi.yml --volume `pwd`:/pkp -it --entrypoint bash pkp:1

upgrade-packages:
	docker run --volume `pwd`:/pkp --rm --user 0 --entrypoint bash -it pkp:1 -c "python3 -m pip install pip-upgrader && pip-upgrade --skip-package-installation"

run:
	docker run --volume `pwd`:/pkp --volume `pwd`/pygeoapi-local-config.yaml:/pygeoapi/local.config.yml -p 5000:80 -it pkp:1
