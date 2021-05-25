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

import unittest
from unittest.mock import MagicMock, patch

from airflow.providers.amazon.aws.operators.emr_stop_notebook_execution import (
    EmrStopNotebookExecutionOperator,
)

STOP_SUCCESS_RETURN = {'ResponseMetadata': {'HTTPStatusCode': 200}}


class TestEmrStopNotebookExecutionOperator(unittest.TestCase):
    def setUp(self):
        # Mock out the emr_client (moto has incorrect response)
        mock_emr_client = MagicMock()
        mock_emr_client.stop_notebook_execution.return_value = STOP_SUCCESS_RETURN

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = mock_emr_client

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)

    def test_execute_stop_notebook_execution_and_does_not_error(self):
        with patch('boto3.session.Session', self.boto3_session_mock):
            operator = EmrStopNotebookExecutionOperator(
                task_id='test_task',
                notebook_execution_id='e-MYDEMONOTEBOOKZBCDEFG',
                aws_conn_id='aws_default',
            )

            operator.execute(None)
