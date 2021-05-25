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

from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class EmrStopNotebookExecutionOperator(BaseOperator):
    """
    Operator to stop an EMR Notebook execution.

    :param notebook_execution_id: id of the EMR Notebook execution to stop. (templated)
    :type notebook_execution_id: str
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """

    template_fields = ['notebook_execution_id']
    template_ext = ()
    ui_color = '#f9c915'

    def __init__(self, *, notebook_execution_id: str, aws_conn_id: str = 'aws_default', **kwargs):
        super().__init__(**kwargs)
        self.notebook_execution_id = notebook_execution_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Dict[str, Any]) -> None:
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        self.log.info('Requesting to stop Notebook execution: %s', self.notebook_execution_id)
        response = emr.stop_notebook_execution(NotebookExecutionId=self.notebook_execution_id)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f'Failed requesting to stop the Notebook execution: {response}')
        else:
            self.log.info('Requested to stop Notebook execution %s ', self.notebook_execution_id)
