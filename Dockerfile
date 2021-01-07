FROM geopython/pygeoapi:latest

RUN mkdir /pkp
WORKDIR /pkp

ADD requirements.txt .
RUN python3 -m pip install -r requirements.txt

ADD . .
RUN python3 setup.py install
