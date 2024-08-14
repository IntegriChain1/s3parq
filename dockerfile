FROM python:3.12

RUN mkdir /app
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN python3 setup.py install

