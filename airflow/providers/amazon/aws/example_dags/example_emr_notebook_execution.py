#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example dag for a AWS EMR Pipeline.

Start a notebook execution then check the notebook execution until it finishes.
"""
import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_start_notebook_execution import EmrStartNotebookExecutionOperator
from airflow.providers.amazon.aws.sensors.emr_notebook_execution import EmrNotebookExecutionSensor
from airflow.utils.dates import days_ago

# [START howto_operator_emr_notebook_execution_env_variables]
NOTEBOOK_ID = os.getenv("NOTEBOOK_ID", "e-MYDEMONOTEBOOKT0ACS9KN5UT")
NOTEBOOK_FILE = os.getenv("NOTEBOOK_FILE", "test.ipynb")
NOTEBOOK_EXECUTION_PARAMS = os.getenv("NOTEBOOK_EXECUTION_PARAMS", '{\"PARAM_1\":10}')
EMR_CLUSTER_ID = os.getenv("EMR_CLUSTER_ID", "j-123456ABCDEFG")
# [END howto_operator_emr_notebook_execution_env_variables]


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='emr_notebook_execution_dag',
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["emr_notebook_execution", "example"],
) as dag:

    # [START howto_operator_emr_notebook_execution_tasks]
    notebook_execution_adder = EmrStartNotebookExecutionOperator(
        task_id='start_notebook_execution',
        aws_conn_id='aws_default',
        editor_id=NOTEBOOK_ID,
        relative_path=NOTEBOOK_FILE,
        notebook_execution_name='test-emr-notebook-execution-airflow-operator',
        notebook_params=NOTEBOOK_EXECUTION_PARAMS,
        execution_engine={'Id': EMR_CLUSTER_ID, 'Type': 'EMR'},
        service_role='EMR_Notebooks_DefaultRole',
        tags=[{"Key": "create-by", "Value": "airflow-operator"}],
    )

    notebook_execution_checker = EmrNotebookExecutionSensor(
        task_id='watch_notebook_execution',
        aws_conn_id='aws_default',
        notebook_execution_id="{{ task_instance.xcom_pull('start_notebook_execution', key='return_value') }}",
    )

    notebook_execution_adder >> notebook_execution_checker
    # [END howto_operator_emr_notebook_execution_tasks]
