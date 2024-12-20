FROM python:3.8-buster
WORKDIR /app
RUN  mkdir /app/utils
COPY ./requirements.txt /app/
RUN pip3 install -r /app/requirements.txt
COPY * /app/
COPY utils/*  /app/utils/

