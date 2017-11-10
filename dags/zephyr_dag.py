# -*- coding: utf-8 -*-
import argparse
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator, S3KeySensor

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

# batches validations and saves job id and output s3 path info in environment variable
t1 = PythonOperator(
    task_id='batch_validations',
    python_callable='run_validations',
    op_args={'s3_path': Variable.get('s3_path'),
             'validation_type': Variable.get('validation_type')},
    dag=dag
)

t2 = PythonOperator(
    task_id='wait_for_queue',
    python_callable='wait_to_queue',
    dag=dag
)

for job in Variable.get('jobs'):
    output_s3_path = 's3://14d-retention/validation_output/prod/{val_type}/{ds}/{job_id}'.format(val_type=Variable.get('validation_type'),
                                                                                                 ds={{ds}},
                                                                                                 job_id=job),
    detect_output_s3_path = S3KeySensor(
        task_id='wait_for_delivery_to_{}'.format(output_s3_path),
        bucket_key=output_s3_path,
        dag=dag
    )
    detect_output_s3_path.set_upstream(t2)

t3 = S3KeySensor(
    task_id='wait_for_delivery',
    bucket_key='',
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
