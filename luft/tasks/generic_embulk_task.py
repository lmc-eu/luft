# -*- coding: utf-8 -*-
"""Generic Embulk Task."""
from pathlib import Path
from typing import Dict, Optional

from luft.common.config import (AWS_ACCESS_KEY_ID, AWS_BUCKET, AWS_ENDPOINT, AWS_SECRET_ACCESS_KEY,
                                BLOB_STORAGE, EMBULK_DEFAULT_TEMPLATE, GCS_APP_NAME,
                                GCS_AUTH_METHOD, GCS_BUCKET, GCS_EMAIL, GCS_JSON_KEYFILE,
                                GCS_P12_KEYFILE)
from luft.common.utils import NoneStr
from luft.tasks.generic_task import GenericTask

import pkg_resources


class GenericEmbulkTask(GenericTask):
    """Generic Embulk JDBC Task."""

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 embulk_template: NoneStr = None, yaml_file: NoneStr = None,
                 env: NoneStr = None, thread_name: NoneStr = None, color: NoneStr = None):
        """Initialize Embulk JDBC Task.

        Parameters:
            name (str): name of task.
            task_type (str): type of task. E.g. embulk-jdbc-load, mongo-load, etc.
            source_system (str): name of source system. Usually name of database.
                Used for better organization especially on blob storage. E.g. jobs, prace, pzr.
            source_subsystem (str): name of source subsystem. Usually name of schema.
                Used for better organization especially on blob storage. E.g. public, b2b.
            env (str): environment - PROD, DEV.
            thread_name(str): name of thread for Airflow parallelization.
            color (str): hex code of color. Airflow operator will have this color.

        """
        super().__init__(name=name, task_type=task_type,
                         source_system=source_system,
                         source_subsystem=source_subsystem,
                         yaml_file=yaml_file,
                         env=env, thread_name=thread_name, color=color)

    def _set_embulk_template(self, embulk_template: Optional[str]):
        """Set Embulk template if specified.

        Else set template from default Embulk templates.

        """
        if embulk_template:
            if Path(embulk_template).exists():
                self.embulk_template = embulk_template
            else:
                raise FileNotFoundError(
                    'File `%s` does not exists.' % embulk_template)
        else:
            tmp_embulk_template = pkg_resources.resource_filename(
                'luft', EMBULK_DEFAULT_TEMPLATE[self.task_type])
            self.embulk_template = tmp_embulk_template.format(
                blob_storage=BLOB_STORAGE)

    def _get_embulk_template(self) -> str:
        """Return Embulk template."""
        return self.embulk_template

    def _get_blob_storage_params(self) -> Dict[str, str]:
        """Get blob storage enviromental variables."""
        blob_storage = BLOB_STORAGE.lower()
        if blob_storage == 'aws':
            return self._get_aws_blob_storage_params()
        elif blob_storage == 'gcs':
            return self._get_gcs_blob_storage_params()
        else:
            raise KeyError(
                'Blob storage %s you specified is not supported!' % blob_storage)

    @staticmethod
    def _get_aws_blob_storage_params() -> Dict[str, str]:
        """Get AWS S3 blob storage parameters."""
        params = {
            'AWS_BUCKET': AWS_BUCKET,
            'AWS_ENDPOINT': AWS_ENDPOINT,
            'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY
        }
        GenericEmbulkTask.check_mandatory(params)
        return params

    def _get_gcs_blob_storage_params(self) -> Dict[str, Optional[str]]:
        """Get GCS blob storage parameters."""
        mandatory_params = {
            'GCS_BUCKET': GCS_BUCKET,
            'GCS_AUTH_METHOD': GCS_AUTH_METHOD
        }
        params = {
            'GCS_APP_NAME': self.get_null_param('application_name', GCS_APP_NAME),
            'GCS_SERVICE_ACCOUNT_EMAIL': self.get_null_param('service_account_email', GCS_EMAIL),
            'GCS_P12_KEYFILE': self.get_null_param('p12_keyfile', GCS_P12_KEYFILE),
            'GCS_JSON_KEYFILE': self.get_null_param('json_keyfile', GCS_JSON_KEYFILE)
        }
        GenericEmbulkTask.check_mandatory(mandatory_params)
        params.update(mandatory_params)
        return params

    @staticmethod
    def get_null_param(param: str, value: str) -> Optional[str]:
        """Return value prefixed with param for nullable params in Embulk template."""
        return f'{param}: {value}' if value else None
