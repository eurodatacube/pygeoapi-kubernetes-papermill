# alternative image repo in case we need to use non official releases:
# FROM eurodatacube/pygeoapi:job-pagination-4aba7ff-20240805
FROM geopython/pygeoapi:0.18.0

RUN apt update \
  && apt --no-install-recommends -y install patch vim-tiny

# Add minor patch to set async as default behavior
COPY async-as-default.patch \
    allow-specifying-job_id-via-request-parameter.patch \
    support-old-jobs-endpoint.patch \
    ./

RUN patch -p0 < async-as-default.patch 
RUN patch -p0 < allow-specifying-job_id-via-request-parameter.patch 
RUN patch -p0 < support-old-jobs-endpoint.patch

RUN mkdir /pkp
WORKDIR /pkp

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

# Stupid workaround:
# Adding views to pygeoapi from here is messy. We have to modify the flask
# app, but then generating the openapi docs imports flask, which fails if
# the openapi docs are missing. Therefore create it here, but empty, such
# that we can even create it *exploding-head-emoji*
RUN touch /pygeoapi/local.openapi.yml

COPY . .
RUN python3 setup.py install
