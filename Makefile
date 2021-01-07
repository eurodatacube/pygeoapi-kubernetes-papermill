build:
	docker build . -t pkp:1

test: build
	docker run --entrypoint pytest pkp:1
	docker run --entrypoint flake8 pkp:1 pygeoapi_kubernetes_papermill tests
	docker run --entrypoint mypy pkp:1 pygeoapi_kubernetes_papermill tests

bash: build
	docker run -it --entrypoint bash pkp:1
