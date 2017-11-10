Using airflow to experiment with DAGs

## Install

    docker pull puckel/docker-airflow

## Run

    docker run -it --rm  -p 8080:8080 -v ${PWD}/dags:/usr/local/airflow/dags -v ${PWD}/config/airflow.cfg:/usr/local/airflow/airflow.cfg -e LOAD_EX=n -e EXECUTOR=Local puckel/docker-airflow bash
