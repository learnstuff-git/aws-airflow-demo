import os
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago

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

S3_BUCKET_NAME = 'raja-test-9186'

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
JOB_FLOW_OVERRIDES = {
    "Name": "Data-Pipeline-" + source + execution_date,
    "ReleaseLabel": "emr-5.33.0",
    # "CustomAmiId": ""
    "LogUri": "s3://{}/logs/emr/".format(S3_BUCKET_NAME),
    "Instances": {
        "Ec2SubnetId": "subnet-0cad43635714cb202",
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m6g.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m6g.xlarge",
                "InstanceCount": 1
            }
        ],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": True,
    },
    'Configurations': [
        {
            'Classification': 'hive-site',
            'Properties': {
                'hive.metastore.glue.catalogid': '174468349935',
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
        },
        {
            'Classification': 'spark-hive-site',
            'Properties': {
                'hive.metastore.glue.catalogid': '174468349935',
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
                'spark.sql.catalogImplementation': 'hive'
            }
        }
    ],
    "Applications": [{"Name": "Spark"}, {"Name": "Hive"}],
    "VisibleToAllUsers": True,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "AutoScalingRole": "EMR_AutoScaling_DefaultRole",
    "ManagedScalingPolicy": {
        "ComputeLimits": {
            "MinimumCapacityUnits": 1,
            "MaximumOnDemandCapacityUnits": 3,
            "MaximumCapacityUnits": 10,
            "MaximumCoreCapacityUnits": 3,
            "UnitType": "Instances"
        }
    },
    "EbsRootVolumeSize": 10
}

S3_URI = "s3://{}/scripts/emr/".format(S3_BUCKET_NAME)

SPARK_TEST_STEPS = [
    {
        'Name': 'setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', '--recursive', S3_URI, '/home/hadoop/']
        }
    },
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '/home/hadoop/testagg.py',
                     's3://{}/raw'.format(S3_BUCKET_NAME),
                     's3://{}/target'.format(S3_BUCKET_NAME)]
        }
    }
]

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
AutoScaling Policy -- not needed. Use managed scaling as it is per source. 
Root Volume -- 10 GB is good enough
Additional EBS Volume -- By default 2 disks with 32 GB is set, with total volume of 64 GB. Set this to 1 32 GB disk
Multiple Steps
airflow tasks pass, even if the emr step fails.(change script name testagg.py and check)
add vpc, subnet -- VPC may not be need, subnet must be passed.
make it generic like CFT
spark test steps and job flow override must be called as config files. seperate from actual dag.
"""
