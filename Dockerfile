FROM geopython/pygeoapi:0.15.0

RUN apt update \
  && apt --no-install-recommends -y install patch vim-tiny

# Add minor patch to set async as default behavior
COPY async-as-default.patch \
    allow-specifying-job_id-via-request-parameter.patch \
    add-support-for-pre-0.11-style-parameter-passing.patch \
    support-old-jobs-endpoint.patch \
    ./

RUN patch -p0 < async-as-default.patch \
  && patch -p0 < allow-specifying-job_id-via-request-parameter.patch \
  && patch -p0 < add-support-for-pre-0.11-style-parameter-passing.patch \
  && patch -p0 < support-old-jobs-endpoint.patch

RUN mkdir /pkp
WORKDIR /pkp

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

COPY . .
RUN python3 setup.py install
