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

import os
import unittest
from unittest.mock import MagicMock, patch

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr_start_notebook_execution import (
    EmrStartNotebookExecutionOperator,
)
from airflow.utils import timezone
from tests.test_utils import AIRFLOW_MAIN_FOLDER

DEFAULT_DATE = timezone.datetime(2017, 1, 1)

START_NOTEBOOK_EXECUTION_SUCCESS_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200},
    'NotebookExecutionId': 'ex-IZZX3NWUC86FISR0SG0E9GGWOXTMD',
}

TEMPLATE_SEARCHPATH = os.path.join(
    AIRFLOW_MAIN_FOLDER, 'tests', 'providers', 'amazon', 'aws', 'config_templates'
)


class TestEmrStartNotebookExecutionOperator(unittest.TestCase):
    def setUp(self):
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client_mock = MagicMock()

        # Mock out the emr_client creator
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_client_mock
        self.boto3_session_mock = MagicMock(return_value=emr_session_mock)

        self.mock_context = MagicMock()

        self.operator = EmrStartNotebookExecutionOperator(
            task_id='test_task',
            aws_conn_id='aws_default',
            editor_id='e-89398BSPG8BYSV81UZFHHRQ15',
            relative_path='test.ipynb',
            notebook_execution_name='test-execution',
            notebook_params='{\"test-param\": \"foo\"}',
            execution_engine={'Id': 'j-V54B6WHVWKED', 'Type': 'EMR'},
            service_role='TestServiceRole',
            notebook_instance_security_group_id='sg-0f35ed0cd66002a0f',
            tags=[{"Key": "test-tag-key", "Value": "test-tag-value"}],
            dag=DAG('test_dag_id', default_args=self.args),
        )

    def test_init(self):
        assert self.operator.editor_id == 'e-89398BSPG8BYSV81UZFHHRQ15'
        assert self.operator.execution_engine['Id'] == 'j-V54B6WHVWKED'
        assert self.operator.relative_path == 'test.ipynb'
        assert self.operator.notebook_params == '{\"test-param\": \"foo\"}'
        assert self.operator.service_role == 'TestServiceRole'
        assert self.operator.aws_conn_id == 'aws_default'

    def test_render_template(self):
        ti = TaskInstance(self.operator, DEFAULT_DATE)
        ti.render_templates()

        assert 'e-89398BSPG8BYSV81UZFHHRQ15' == getattr(self.operator, 'editor_id')
        assert 'test.ipynb' == getattr(self.operator, 'relative_path')
        assert 'test-execution' == getattr(self.operator, 'notebook_execution_name')
        assert '{\"test-param\": \"foo\"}' == getattr(self.operator, 'notebook_params')
        assert {'Id': 'j-V54B6WHVWKED', 'Type': 'EMR'} == getattr(self.operator, 'execution_engine')
        assert 'TestServiceRole' == getattr(self.operator, 'service_role')
        assert 'sg-0f35ed0cd66002a0f' == getattr(self.operator, 'notebook_instance_security_group_id')
        assert [{"Key": "test-tag-key", "Value": "test-tag-value"}] == getattr(self.operator, 'tags')

    def test_execute_returns_notebook_execution_id(self):
        self.emr_client_mock.start_notebook_execution.return_value = START_NOTEBOOK_EXECUTION_SUCCESS_RETURN

        with patch('boto3.session.Session', self.boto3_session_mock):
            assert self.operator.execute(self.mock_context) == 'ex-IZZX3NWUC86FISR0SG0E9GGWOXTMD'
