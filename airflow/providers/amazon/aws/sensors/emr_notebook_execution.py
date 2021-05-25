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

from typing import Any, Dict, Iterable, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr_base import EmrBaseSensor


class EmrNotebookExecutionSensor(EmrBaseSensor):
    """
    Asks for the state of the NotebookExecution until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    :param execution_id: notebook execution_id to check the state of
    :type execution_id: str
    :param target_states: the target states, sensor waits until
        notebook execution reaches any of these states
    :type target_states: list[str]
    :param failed_states: the failure states, sensor fails when
        notebook execution reaches any of these states
    :type failed_states: list[str]
    """

    template_fields = ['notebook_execution_id', 'target_states', 'failed_states']
    template_ext = ()

    def __init__(
        self,
        *,
        notebook_execution_id: str,
        target_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.notebook_execution_id = notebook_execution_id
        self.target_states = target_states or ['FINISHED']
        self.failed_states = failed_states or ['FAILED', 'STOPPED']

    def poke(self, context):
        response = self.get_emr_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = self.state_from_response(response)
        self.log.info('Notebook execution is %s', state)

        if state in self.target_states:
            return True

        if state in self.failed_states:
            final_message = 'Notebook execution failed'
            failure_message = self.failure_message_from_response(response)
            if failure_message:
                final_message = failure_message
            raise AirflowException(final_message)

        return False

    def get_emr_response(self):
        """
        Make an API call with boto3 and get notebook execution details.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_notebook_execution

        :return: response
        :rtype: dict[str, Any]
        """
        emr = self.get_hook().get_conn()
        self.log.info('Poking notebook execution %s', self.notebook_execution_id)
        return emr.describe_notebook_execution(NotebookExecutionId=self.notebook_execution_id)

    @staticmethod
    def state_from_response(response: Dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :type response: dict[str, Any]
        :return: current state of the cluster
        :rtype: str
        """
        return response['NotebookExecution']['Status']

    @staticmethod
    def failure_message_from_response(response):
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :type response: dict[str, Any]
        :return: failure message
        :rtype: Optional[str]
        """
        state = response['NotebookExecution']['Status']
        if state not in ['FAILED', 'STOPPED']:
            return None
        last_state_change_reason = response['NotebookExecution']['LastStateChangeReason']
        if last_state_change_reason:
            return 'Execution failed with reason: ' + last_state_change_reason
        return None
