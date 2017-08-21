# -*- coding: utf-8 -*-
import argparse
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator

AIRFLOW_EMAIL = os.environ.get("AIRFLOW_EMAIL")

default_args = {
    'owner': 'gerdie',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': [AIRFLOW_EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('submit_validations', default_args=default_args)

t1 = PythonOperator(
    task_id='batch_validations',
    python_callable='run_validations',
    op_args={'s3_path': Variable.get('s3_path'),
             'validation_type': Variable.get('validation_type')},
    dag=dag
)

t2 = PythonOperator(
    task_id='wait_for_queue',
    python_callable='send_to_argo',
    dag=dag
)

t3 = PythonOperator(
    task_id='block_until_finished_and_deliver',
    python_callable='block_until_finished_and_deliver',
    dag=dag
)

t4 = PythonOperator(
    task_id='qa',
    python_callable='qa',
    dag=dag
)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)


def execute():
    arg_parser = argparse.ArgumentParser(description='Submits a list of items for validation, waits for their '
                                                     'results, and saves them to S3. Validation items are specified '
                                                     'by an S3 URI.')
    arg_parser.add_argument('--s3-path', required=True,
                            help='The full S3 path of the validation items. Must be a line-separated file, '
                                 'or a folder of line-separated files.')
    arg_parser.add_argument('--validation-type', required=True)

    args = arg_parser.parse_args()

    Variable['s3_path'] = args.s3_path
    Variable['validation_type'] = args.validation-type

if __name__ == '__main__':
    execute()
