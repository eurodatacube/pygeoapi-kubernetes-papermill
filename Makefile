test: build
	docker run --entrypoint pytest pkp:1
	docker run --entrypoint flake8 pkp:1 pygeoapi_kubernetes_papermill tests
	docker run --entrypoint mypy pkp:1 pygeoapi_kubernetes_papermill tests

build:
	docker build . -t pkp:1

test-watch: build
	docker run --volume `pwd`:/pkp --entrypoint ptw pkp:1

bash: build
	docker run --volume `pwd`:/pkp -it --entrypoint bash pkp:1
