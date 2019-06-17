FROM python:3.7

RUN mkdir /app
COPY . /app
WORKDIR /app
RUN python3 setup.py install

