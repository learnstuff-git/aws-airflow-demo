import os
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


####DAG_ID = os.path.basename(__file__).replace('.py', '')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

S3_BUCKET_NAME = 'telenor-se-airflow-sat'

dag = DAG(
    'data_pipeline-2',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)

execution_date = "{{ execution_date }}"
####source = "{{source_name}}"
source = "test-"
# noinspection PyCompatibility

def get_object(key, bucket_name):
    """
    Load S3 object as JSON
    """

    hook = S3Hook()
    content_object = hook.get_key(key=key, bucket_name=bucket_name)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


JOB_FLOW_OVERRIDES = get_object('job_flow_overrides/job_flow_overrides_ig.json', work_bucket)

S3_URI = "s3://{}/scripts/emr/".format(S3_BUCKET_NAME)

SPARK_TEST_STEPS = get_object('emr_steps/test/emr_steps.json', work_bucket)

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)
"""
last_step = len(SPARK_TEST_STEPS) - 1

step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)
"""

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)

cluster_creator >> step_adder >> step_checker

"""
To Do:
Task instance group issue : Minimum 1 instance is needed at the start
Multiple Steps
airflow tasks pass, even if the emr step fails.(change script name testagg.py and check)
make it generic like CFT
spark test steps and job flow override must be called as config files. seperate from actual dag.


Fixed
AutoScaling Policy -- not needed. Use managed scaling as it is per source.
Root Volume -- 10 GB is good enough
Additional EBS Volume -- By default 2 disks with 32 GB is set, with total volume of 64 GB. Set this to 1 32 GB disk
add vpc, subnet -- VPC may not be need, subnet must be passed.

"""
