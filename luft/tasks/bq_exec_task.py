# -*- coding: utf-8 -*-
"""BigQuery exec Task."""
from pathlib import Path
from typing import Dict, List, Union

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2.service_account import Credentials

from jinja2 import Template

from luft.common.config import BQ_CREDENTIALS_FILE, BQ_JOB_ID_PREFIX, BQ_LOCATION, BQ_PROJECT_ID
from luft.common.logger import setup_logger
from luft.common.utils import NoneStr
from luft.tasks.generic_task import GenericTask

# Setup logger
logger = setup_logger('common', 'INFO')
NoneDict = Union[Dict[str, str], None]


class BQExecTask(GenericTask):
    """BQ exec Task."""

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 sql_folder: NoneStr = None, sql_files: Union[List[str], None] = None,
                 project_id: NoneStr = None, location: NoneStr = None, yaml_file: NoneStr = None,
                 env: NoneStr = None, thread_name: NoneStr = None, color: NoneStr = None):
        """Initialize BigQuery JDBC Task.

        Attributes:
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
        self.sql_folder = sql_folder or ''
        self.sql_files = sql_files or ['']
        self.bq_project_id = self._get_project_id(project_id)
        self.bq_location = self._get_location(location)
        self.bq_client = self._init_bq_client()  # Initialize BQ client
        super().__init__(name=name, task_type=task_type,
                         source_system=source_system,
                         source_subsystem=source_subsystem,
                         yaml_file=yaml_file,
                         env=env, thread_name=thread_name, color=color)

    def __call__(self, ts: str, env: NoneStr = None):
        """Make class callable.

        Attributes:
            ts (str): time of valid.

        """
        env_vars = self.get_env_vars(ts, env)
        self._run_bq_command(self.sql_folder, self.sql_files, env_vars)

    def get_env_vars(self, ts: str, env: NoneStr = None) -> Dict[str, str]:
        """Get Docker enviromental variables."""
        super_env_dict = super().get_env_vars(ts=ts, env=env)
        env_dict = {
            'BQ_PROJECT_ID': self.bq_project_id,
            'BQ_LOCATION': self.bq_location
        }
        clean_dict = self.clean_dictionary(env_dict)
        clean_dict.update(super_env_dict)
        return clean_dict

    def get_bq_client(self) -> bigquery.Client:
        """Return BigQuery client."""
        return self.bq_client

    def _prepare_scripts(self, sql_folder: str, sql_files: List[str]):
        scripts = []
        if len(sql_files) > 0:
            for script in sql_files:
                scripts.append(Path(sql_folder) / script)
            return scripts

    def _init_bq_client(self) -> bigquery.Client:
        """Init BigQuery client."""
        bq_cred = Credentials.from_service_account_file(BQ_CREDENTIALS_FILE)
        return bigquery.Client(project=self.bq_project_id, credentials=bq_cred)

    def _get_project_id(self, project_id: NoneStr = None):
        """Get BigQuery project id."""
        bq_project_id = project_id or BQ_PROJECT_ID
        self.check_mandatory({'BQ_PROJECT_ID': bq_project_id})
        return bq_project_id

    def _get_location(self, location: NoneStr = None):
        """Get BigQuery location."""
        bq_location = location or BQ_LOCATION
        self.check_mandatory({'BQ_LOCATION': bq_location})
        return bq_location

    def _get_sql_commands(self, sql_folder: str, sql_files: List[str],
                          env_vars: NoneDict = None):
        exec_scripts = self._prepare_scripts(sql_folder, sql_files)
        cmds: List[str] = []
        for file_path in exec_scripts:
            if file_path.exists():
                with open(file_path) as file_:
                    template = Template(file_.read())
                rendered = template.render(env_vars)
                # split & strip rendered template
                sql_cmds = list(map(str.strip, rendered.split(';')))
                # remove blank commands
                sql_cmds = list(filter(None, sql_cmds))
                cmds.extend(sql_cmds)
            else:
                raise FileNotFoundError(f'File `{file_}` does not exist.')
        return cmds

    def _create_dataset(self, dataset_id: str):
        """Create dataset.

        Parameters:
            dataset_id (str): identifier of dataset.

        """
        if not self._dataset_exists(dataset_id):
            dataset_id = f'{self.bq_client.project}.{dataset_id}'
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.bq_location
            dataset = self.bq_client.create_dataset(dataset)
            logger.info(f'Dataset {dataset_id} has been created.')

    def _dataset_exists(self, dataset_id: str) -> bool:
        """Check if dataset exits.

        Parameters:
            dataset_id (str): identifier of dataset.

        """
        try:
            self.bq_client.get_dataset(dataset_id)
            logger.info(f'Dataset {dataset_id} already exists.')
            return True
        except NotFound:
            logger.info(f'Dataset {dataset_id} does not exist.')
            return False

    def _run_bq_command(self, sql_folder: str, sql_files: List[str],
                        env_vars: NoneDict = None):
        """Run BigQuery command."""
        queries = self._get_sql_commands(sql_folder, sql_files, env_vars)
        for query in queries:
            query_job = self.bq_client.query(
                query,
                job_id_prefix=BQ_JOB_ID_PREFIX,
                project=self.bq_project_id,
                location=self.bq_location
            )  # API request - starts the query
            start_msg = f'Starting job {query_job.job_id}'
            logger.info('#' * len(start_msg))
            logger.info(start_msg)
            logger.info('-' * len(start_msg))
            for line in query_job.query.split('\n'):
                logger.info(line)
            query_job.result()
            duration = query_job.ended - query_job.started
            end_msg = (f'Job {query_job.job_id} finished.')
            logger.info('-' * len(start_msg))
            logger.info(end_msg)
            logger.info(
                f'{query_job.state}. It took {duration.total_seconds()} sec.')
            logger.info('#' * len(start_msg))
