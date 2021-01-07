FROM geopython/pygeoapi:latest

ADD async-as-default.patch .
RUN apt update \
  && apt install patch \
  && patch -p0 < async-as-default.patch

RUN mkdir /pkp
WORKDIR /pkp

ADD requirements.txt .
RUN python3 -m pip install -r requirements.txt

ADD . .
RUN python3 setup.py install
