FROM geopython/pygeoapi:latest

# Add minor patch to set async as default behavior
COPY async-as-default.patch .
RUN apt update \
  && apt install patch \
  && patch -p0 < async-as-default.patch

RUN mkdir /pkp
WORKDIR /pkp

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt

COPY . .
RUN python3 setup.py install
