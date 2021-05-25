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
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class EmrStartNotebookExecutionOperator(BaseOperator):
    """
    An operator that starts a notebook execution to an existing EMR job_flow.

    :param editor_id: Id of the emr notebook to run (with prefix e-). (templated)
    :type editor_id: Optional[str]
    :param relative_path: Relative path of the notebook file to run. (templated)
    :type relative_path: Optional[str]
    :param notebook_execution_name: The name for the execution to be created (templated)
    :type notebook_execution_name: Optional[str]
    :param execution_engine: A JSON string to specify the EMR cluster,
        e.g. {'Id': 'j-123', 'Type': 'EMR'}. (templated)
    :type execution_engine: Optional[str]
    :param service_role: The service role name or ARN needed by the EMR service
        to operate on your behalf. (templated)
    :type service_role: Optional[str]
    :param notebook_instance_security_group_id: Security group ID for the EC2 instance that runs
        the notebook server. If not supplied, a default security group will be used. (templated)
    :type notebook_instance_security_group_id: Optional[str]
    :param tags: A list of tags to be applied to the execution. (templated)
    :type tags: Optional[List[Dict[str, str]]]
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param do_xcom_push: if True, notebook_execution_id is pushed to XCom with key notebook_execution_id.
    :type do_xcom_push: bool
    """

    template_fields = [
        'editor_id',
        'relative_path',
        'notebook_execution_name',
        'notebook_params',
        'execution_engine',
        'service_role',
        'notebook_instance_security_group_id',
        'tags',
    ]
    template_ext = ('.json',)
    ui_color = '#f9c915'

    def __init__(
        self,
        *,
        editor_id: str,
        relative_path: str,
        notebook_execution_name: Optional[str] = None,
        notebook_params: Optional[str] = None,
        execution_engine: Dict[str, str],
        service_role: str,
        notebook_instance_security_group_id: Optional[str] = None,
        tags: Optional[List[Dict[str, str]]] = None,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id

        self.editor_id = editor_id
        self.relative_path = relative_path
        self.notebook_execution_name = notebook_execution_name
        self.notebook_params = notebook_params
        self.execution_engine = execution_engine
        self.service_role = service_role
        self.notebook_instance_security_group_id = notebook_instance_security_group_id
        self.tags = tags

    def execute(self, context: Dict[str, Any]) -> str:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr = emr_hook.get_conn()

        inputs = {
            "EditorId": self.editor_id,
            "RelativePath": self.relative_path,
            "ExecutionEngine": self.execution_engine,
            "ServiceRole": self.service_role,
        }

        if self.notebook_execution_name:
            inputs["NotebookExecutionName"] = self.notebook_execution_name

        if self.notebook_params:
            inputs["NotebookParams"] = self.notebook_params

        if self.notebook_instance_security_group_id:
            inputs["NotebookInstanceSecurityGroupId"] = self.notebook_instance_security_group_id

        if self.tags:
            inputs["Tags"] = self.tags

        response = emr.start_notebook_execution(**inputs)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f'Starting notebook execution failed: {response}')
        else:
            notebook_execution_id = response['NotebookExecutionId']
            self.log.info('Started a notebook execution %s', notebook_execution_id)
            if self.do_xcom_push:
                context['ti'].xcom_push(key='notebook_execution_id', value=notebook_execution_id)
            return notebook_execution_id
