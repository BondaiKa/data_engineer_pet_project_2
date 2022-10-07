FROM jupyter/pyspark-notebook

ARG WORKDIR=/data_engineer_pet_project_2

RUN mkdir datalake


WORKDIR $WORKDIR
COPY data_engineer_pet_project_2 .
COPY test ./test
