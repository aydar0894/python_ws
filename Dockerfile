FROM registry.gitlab.com/darqubie/shared_registry/python3-flask:latest
MAINTAINER greenclaw "greenclawanderson@gmail.com"


RUN pip3 install scipy
RUN pip3 install flask_cors

RUN mkdir /app


WORKDIR /app

COPY ./* /app/
# RUN mkdir /app/TESTING_DATA
# COPY ./TESTING_DATA/* /app/TESTING_DATA/


CMD python3 ./main.py
