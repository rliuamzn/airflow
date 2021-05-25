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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr_notebook_execution import EmrNotebookExecutionSensor

DESCRIBE_NOTEBOOK_EXECUTION_RUNNING_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    "NotebookExecution": {
        "NotebookExecutionId": "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5",
        "EditorId": "e-2ZTI303F7CESD8BURE1Z9R3QS",
        "ExecutionEngine": {
            "Id": "j-3WC9BUESQ6EA",
            "Type": "EMR",
            "MasterInstanceSecurityGroupId": "sg-08e7ae5244d06ef64",
        },
        "NotebookExecutionName": "test-execution",
        "NotebookParams": "",
        "Status": "RUNNING",
        "StartTime": 1621969343.792,
        "EndTime": 1621969365.824,
        "Arn": "arn:aws:elasticmapreduce:us-east-1:123456789012:notebook-execution/"
        + "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5",
        "NotebookInstanceSecurityGroupId": "sg-0f35ed0cd66002a0f",
        "Tags": [],
    },
}

DESCRIBE_NOTEBOOK_EXECUTION_FINISHED_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    "NotebookExecution": {
        "NotebookExecutionId": "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5",
        "EditorId": "e-2ZTI303F7CESD8BURE1Z9R3QS",
        "ExecutionEngine": {
            "Id": "j-3WC9BUESQ6EA",
            "Type": "EMR",
            "MasterInstanceSecurityGroupId": "sg-08e7ae5244d06ef64",
        },
        "NotebookExecutionName": "test-execution",
        "NotebookParams": "",
        "Status": "FINISHED",
        "StartTime": 1621969343.792,
        "EndTime": 1621969365.824,
        "Arn": "arn:aws:elasticmapreduce:us-east-1:123456789012:notebook-execution/"
        + "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5",
        "OutputNotebookURI": "s3://my-test-bucket/test-folder/e-2ZTI303F7CESD8BURE1Z9R3QS/"
        + "executions/ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5/test.ipynb",
        "LastStateChangeReason": "Execution is finished for cluster j-3WC9BUESQ6EA.",
        "NotebookInstanceSecurityGroupId": "sg-0f35ed0cd66002a0f",
        "Tags": [],
    },
}

DESCRIBE_NOTEBOOK_EXECUTION_STOPPED_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    "NotebookExecution": {
        "NotebookExecutionId": "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5",
        "EditorId": "e-5SB80N6HCVPKT0LPH64C1US0L",
        "ExecutionEngine": {
            "Id": "j-3N9TFX0GSZE6H",
            "Type": "EMR",
            "MasterInstanceSecurityGroupId": "sg-08e7ae5244d06ef64",
        },
        "NotebookExecutionName": "test-execution",
        "NotebookParams": "",
        "Status": "STOPPED",
        "StartTime": 1621970206.627,
        "EndTime": 1621970227.281,
        "Arn": "arn:aws:elasticmapreduce:us-east-1:123456789012:notebook-execution/"
        + "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5",
        "OutputNotebookURI": "s3://my-test-bucket/test-folder/e-5SB80N6HCVPKT0LPH64C1US0L/executions/"
        + "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5/test.ipynb",
        "LastStateChangeReason": "Execution is stopped for cluster j-3N9TFX0GSZE6H.",
        "NotebookInstanceSecurityGroupId": "sg-0f35ed0cd66002a0f",
        "Tags": [],
    },
}

DESCRIBE_NOTEBOOK_EXECUTION_FAILED_RETURN = {
    'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '8dee8db2-3719-11e6-9e20-35b2f861a2a6'},
    "NotebookExecution": {
        "NotebookExecutionId": "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5",
        "EditorId": "e-89398BSPG8BYSV81UZFHHRQ15",
        "ExecutionEngine": {
            "Id": "j-V54B6WHVWKED",
            "Type": "EMR",
            "MasterInstanceSecurityGroupId": "sg-08e7ae5244d06ef64",
        },
        "NotebookExecutionName": "test-execution",
        "NotebookParams": "",
        "Status": "FAILED",
        "StartTime": 1621955725.619,
        "EndTime": 1621955807.32,
        "Arn": "arn:aws:elasticmapreduce:us-east-1:123456789012:notebook-execution/"
        + "ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5",
        "OutputNotebookURI": "s3://my-test-bucket/test-folder/e-89398BSPG8BYSV81UZFHHRQ15/"
        + "executions/ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5/test.ipynb",
        "LastStateChangeReason": "Execution has failed for cluster j-V54B6WHVWKED.",
        "NotebookInstanceSecurityGroupId": "sg-0f35ed0cd66002a0f",
        "Tags": [],
    },
}


class TestEmrNotebookExecutionSensor(unittest.TestCase):
    def setUp(self):
        self.emr_client_mock = MagicMock()
        self.sensor = EmrNotebookExecutionSensor(
            task_id='test_task',
            poke_interval=0,
            notebook_execution_id='ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5',
            aws_conn_id='aws_default',
        )

        mock_emr_session = MagicMock()
        mock_emr_session.client.return_value = self.emr_client_mock

        # Mock out the emr_client creator
        self.boto3_session_mock = MagicMock(return_value=mock_emr_session)
        self.notebook_execution_id = 'ex-IZZX0PTECXPX4UJV0YVE8M7UUSLR5'

    def test_notebook_execution_completed(self):
        self.emr_client_mock.describe_notebook_execution.side_effect = [
            DESCRIBE_NOTEBOOK_EXECUTION_RUNNING_RETURN,
            DESCRIBE_NOTEBOOK_EXECUTION_FINISHED_RETURN,
        ]
        with patch('boto3.session.Session', self.boto3_session_mock):
            self.sensor.execute(None)

            assert self.emr_client_mock.describe_notebook_execution.call_count == 2
            calls = [
                unittest.mock.call(NotebookExecutionId=self.notebook_execution_id),
                unittest.mock.call(NotebookExecutionId=self.notebook_execution_id),
            ]
            self.emr_client_mock.describe_notebook_execution.assert_has_calls(calls)

    def test_notebook_execution_stopped(self):
        self.emr_client_mock.describe_notebook_execution.side_effect = [
            DESCRIBE_NOTEBOOK_EXECUTION_RUNNING_RETURN,
            DESCRIBE_NOTEBOOK_EXECUTION_STOPPED_RETURN,
        ]
        with patch('boto3.session.Session', self.boto3_session_mock):
            with pytest.raises(AirflowException):
                self.sensor.execute(None)

    def test_notebook_execution_failed(self):
        self.emr_client_mock.describe_notebook_execution.side_effect = [
            DESCRIBE_NOTEBOOK_EXECUTION_RUNNING_RETURN,
            DESCRIBE_NOTEBOOK_EXECUTION_FAILED_RETURN,
        ]
        with patch('boto3.session.Session', self.boto3_session_mock):
            with pytest.raises(AirflowException):
                self.sensor.execute(None)
