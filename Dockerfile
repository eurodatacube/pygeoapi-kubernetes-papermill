# TODO: remove patch for job_id after next image update, it's upstream now
FROM geopython/pygeoapi:0.10.1

# Add minor patch to set async as default behavior
COPY async-as-default.patch allow-specifying-job_id-via-request-parameter.patch ./
RUN apt update \
  && apt --no-install-recommends -y install patch vim-tiny \
  && patch -p0 < async-as-default.patch \
  && patch -p0 < allow-specifying-job_id-via-request-parameter.patch

RUN mkdir /pkp
WORKDIR /pkp

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

COPY . .
RUN python3 setup.py install
