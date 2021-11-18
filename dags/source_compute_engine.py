import os
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago

"""
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
"""

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

S3_BUCKET_NAME = 'raja-test-9186/logs/emr/'

dag = DAG(
    'data_pipeline-2',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)

execution_date = "{{ execution_date }}"
####source = "{{source_name}}"
source = "test-"
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
                "InstanceCount": 2,
                # "AutoScalingPolicy":
                #     {
                #         "Constraints":
                #             {
                #                 "MinCapacity": 2,
                #                 "MaxCapacity": 10
                #             },
                #         "Rules":
                #             [
                #                 {
                #                     "Name": "Default-scale-out",
                #                     "Description": "Replicates the default scale-out rule in the console for YARN memory.",
                #                     "Action": {
                #                         "Market": "ON_DEMAND"|"SPOT",
                #                         "SimpleScalingPolicyConfiguration": {
                #                             "AdjustmentType": "CHANGE_IN_CAPACITY",
                #                             "ScalingAdjustment": 1,
                #                             "CoolDown": 300
                #                         }
                #                     },
                #                     "Trigger": {
                #                         "CloudWatchAlarmDefinition": {
                #                             "ComparisonOperator": "LESS_THAN",
                #                             "EvaluationPeriods": 1,
                #                             "MetricName": "YARNMemoryAvailablePercentage",
                #                             "Namespace": "AWS/ElasticMapReduce",
                #                             "Period": 300,
                #                             "Threshold": 15,
                #                             "Statistic": "AVERAGE",
                #                             "Unit": "PERCENT",
                #                             "Dimensions": [
                #                                 {
                #                                     "Key": "JobFlowId",
                #                                     "Value": "${emr.clusterId}"
                #                                 }
                #                             ]
                #                         }
                #                     }
                #                 }
                #             ]
                #     }
            },
            {
                "Name": "Task nodes",
                "Market": "SPOT",
                "InstanceRole": "TASK",
                "InstanceType": "m6g.xlarge",
                "InstanceCount": 0,
                "AutoScalingPolicy":
                    {
                        "Constraints":
                            {
                                "MinCapacity": 0,
                                "MaxCapacity": 10
                            }
                    },
                "Rules":
                    [
                        {
                            "Name": "Default-scale-out",
                            "Description": "Replicates the default scale-out rule in the console for YARN memory.",
                            "Action": {
                                "SimpleScalingPolicyConfiguration": {
                                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                                    "ScalingAdjustment": 1,
                                    "CoolDown": 300
                                }
                            },
                            "Trigger": {
                                "CloudWatchAlarmDefinition": {
                                    "ComparisonOperator": "LESS_THAN",
                                    "EvaluationPeriods": 1,
                                    "MetricName": "YARNMemoryAvailablePercentage",
                                    "Namespace": "AWS/ElasticMapReduce",
                                    "Period": 300,
                                    "Threshold": 15,
                                    "Statistic": "AVERAGE",
                                    "Unit": "PERCENT",
                                    "Dimensions": [
                                        {
                                            "Key": "JobFlowId",
                                            "Value": "${emr.clusterId}"
                                        }
                                    ]
                                }
                            }
                        }
                    ]

            }
        ],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": True
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
    "EbsRootVolumeSize": 16
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
AutoScaling Policy
Root Volume
Additional EBS Volume
Multiple Steps
airflow tasks pass, even if the emr step fails.(change script name testagg.py and check)
add vpc, subnet
make it generic like CFT
spark test steps and job flow override must be called as config files. seperate from actual dag.
"""
