# -*- coding: utf-8 -*-

######################################################
# PROJECT : Anuvaad Workflow : Airflow
# AUTHOR  : Tarento Technologies
# DATE    : Feb 13, 2020
######################################################


import random
import json
import yaml
import datetime as dt

import airflow
from airflow.models import DAG
from airflow.models import Variable
from airflow.models import XCom

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.sensors import HdfsSensor
from airflow.utils.db import provide_session


'''
This is a ML workflow created for the Anuvaad project.
Please read the general documentation for Anuvaad before going through the code.

The steps includes (& not restricted to) :
    1. Token & Sentence Extraction
    2. Sentences Correction
    3. Sentences Augmentation
    4. Model Training
    5. Model Inference
    6. Model Evaluation

Few of the operators are defined as Dummy, which can be developed later, based
on the specific requirements.

The code has been tested on HDP 3.1.4 and Airflow 1.10.7 stack.

'''

'''
---------------------------------------
READ THE CONFIG & LOAD THE VARIABLE
---------------------------------------
'''
CONFIG_FILE   = '/usr/local/airflow/dags/config/config.yml'
CONFIG_DB_KEY = 'anuvaad_config'

with open(CONFIG_FILE) as file:
    default_config = yaml.load(file, Loader=yaml.FullLoader)

default_config = json.dumps(default_config, default=str)
config = Variable.setdefault(CONFIG_DB_KEY, default_config, deserialize_json=True)
config = yaml.load(CONFIG_FILE, Loader=yaml.FullLoader)


'''
---------------------------------------
DEFAULT ARGS
---------------------------------------
'''
default_args = {
    'owner': 'aravinth',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['aravinth.bheemaraj@tarento.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5),
    'provide_context': True,
    'concurrency': 1,
    'max_active_runs': 1,
    # 'queue': 'default',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2020, 6, 1),
}

'''
---------------------------------------
BRANCHING VARIABLES
---------------------------------------
'''
unimplemented_opts = ['txt', 'pdf', 'docx']
manual_check_options = ['yes', 'no']


'''
Choose the branching based on the rules.
'''
def choose_mc_branch(**kwargs):
    ti = kwargs['ti']

    if random.choice(manual_check_options) == 'yes':
        return 'csv_manual_correction'
    else:
        return 'join'


'''
Choose the branching based on the file extension.
When specific implementations are available, replace
the default implementation with it.
'''
def choose_proc_branch(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='parse_input_filetype')
    if xcom_value in unimplemented_opts:
        ret_val = xcom_value + '_processing'
    elif xcom_value == 'csv':
        ret_val = 'csv_token_extractor'
    else:
        ret_val = 'txt_processing'
    return ret_val

'''
Cleans up all the XCOM variables used in this DAG after each run.
'''
@provide_session
def cleanup_xcom(context, session=None):
    dag_id = context['dag'].dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


dag = DAG(
    dag_id='anuvaad_dag',
    default_args=default_args,
    schedule_interval="@daily",
    on_success_callback=cleanup_xcom,
)



'''
---------------------------------------
HDFS SENSOR TO WAIT UNTIL INPUT FILE IS
AVAILABLE IN THE SPECIFIED DIR.
---------------------------------------
'''
#source_data_sensor = HdfsSensor(
#    task_id='source_data_sensor',
#    filepath=kwargs['dag_run'].conf['anuvaad_input_dir'],
#    poke_interval=10,
#    timeout=5,
#    dag=dag
#)


'''
---------------------------------------
PARSE THE FILETYPE
---------------------------------------
'''
parse_input_filetype = BashOperator(
    task_id='parse_input_filetype',
    # bash_command="hadoop fs -ls {{ var.json.anuvaad_config.anuvaad_input_dir }} | cut -d '.' -f2 | tail -1",
    bash_command="hadoop fs -ls {{ var.value.anuvaad_input_dir }} | cut -d '.' -f2 | tail -1",
    xcom_push=True,
    dag=dag,
)


'''
---------------------------------------
BRANCHING FOR FILE TYPE PATH SELECTION
---------------------------------------
'''
filetype_branching = BranchPythonOperator(
    task_id='filetype_branching',
    python_callable=choose_proc_branch,
    dag=dag,
)


'''
---------------------------------------
BRANCHING FOR MANUAL CHECK CASE
---------------------------------------
'''
manual_check_branching = BranchPythonOperator(
    task_id='manual_check_branching',
    python_callable=choose_mc_branch,
    dag=dag,
)


'''
---------------------------------------
JOIN OPERATOR
---------------------------------------
'''
join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag,
)


'''
---------------------------------------
CSV TOKEN EXTRACTOR
---------------------------------------
'''
csv_token_extractor = SparkSubmitOperator(
    task_id='csv_token_extractor',
    conn_id='spark_default',
    application='hdfs://node1.anuvaad.dev:8020/user/root/anuvaad/anuvaad_tool_1_token_extractor.py',
    name='csv tool 1 : token extractor',
    application_args=[default_config],
    dag=dag
)


'''
---------------------------------------
MANUAL VERIFICATION SENSOR
---------------------------------------
'''
tokens_manual_verfication_sensor = BashOperator(
        task_id='tokens_manual_verfication_sensor',
        bash_command='echo Entering sleep mode.. waiting for sometime && sleep 60',
        dag=dag
)


'''
---------------------------------------
CSV SENTENCE EXTRACTOR
---------------------------------------
'''
csv_sentence_extractor = SparkSubmitOperator(
    task_id='csv_sentence_extractor',
    conn_id='spark_default',
    application='hdfs://node1.anuvaad.dev:8020/user/root/anuvaad/anuvaad_tool_1_sentence_extractor.py',
    name='csv tool 1 : sentence extractor',
    application_args=[default_config],
    dag=dag
)


'''
---------------------------------------
CSV MANUAL CORRECTION
---------------------------------------
'''
csv_manual_correction = BashOperator(
        task_id='csv_manual_correction',
        bash_command='echo Performed a manual correction!',
        dag=dag
)


'''
---------------------------------------
UNIMPLEMENTED OPTIONS - FOR FUTURE
---------------------------------------
'''
for option in unimplemented_opts:
    t = DummyOperator(
        task_id=option + '_processing',
        dag=dag,
    )

    dummy_follow = DummyOperator(
        task_id=option + '_post_processing',
        dag=dag,
    )

    filetype_branching >> t >> dummy_follow >> join


'''
---------------------------------------
MODEL TRAINING
---------------------------------------
'''
model_training = DummyOperator(
    task_id='model_training',
    dag=dag,
)


'''
---------------------------------------
MODEL INFERENCE
---------------------------------------
'''
model_inference = DummyOperator(
    task_id='model_inference',
    dag=dag,
)


'''
---------------------------------------
MODEL EVALUATION
---------------------------------------
'''
model_evaluation = DummyOperator(
    task_id='model_evaluation',
    dag=dag,
)


'''
---------------------------------------
DEFINE THE WORKFLOW
---------------------------------------
'''
#source_data_sensor >> parse_input_filetype >> filetype_branching
parse_input_filetype >> filetype_branching
filetype_branching >> csv_token_extractor >> tokens_manual_verfication_sensor >> csv_sentence_extractor >> manual_check_branching
manual_check_branching >> csv_manual_correction
manual_check_branching >> join
csv_manual_correction >> join
join >> model_training >> model_inference >> model_evaluation
