test:
	docker build . -t pkp:1
	docker run --entrypoint pytest pkp:1
	docker run --entrypoint flake8 pkp:1 pygeoapi_kubernetes_papermill tests
	docker run --entrypoint mypy pkp:1 pygeoapi_kubernetes_papermill tests
