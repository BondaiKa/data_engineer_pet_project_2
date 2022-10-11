FROM jupyter/pyspark-notebook

ARG WORKDIR=/data_engineer_pet_project_2

WORKDIR $WORKDIR
COPY data_engineer_pet_project_2 .
COPY test ./test
COPY data_engineer_pet_project_2/config/files/docker_config.yml ./config/files/default.yml