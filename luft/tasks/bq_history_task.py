# -*- coding: utf-8 -*-
"""BigQuery history Task."""
from pathlib import Path
from typing import List, Union

from google.cloud import bigquery

from luft.common.config import PATH_PREFIX, GCS_BUCKET
from luft.common.logger import setup_logger
from luft.common.utils import NoneStr, get_path_prefix
from luft.tasks.bq_exec_task import BQExecTask

# Setup logger
logger = setup_logger('common', 'INFO')


class BQHistoryTask(BQExecTask):
    """BQ exec Task."""

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 path_prefix: NoneStr = None,
                 yaml_file: NoneStr = None, env: NoneStr = None, thread_name: NoneStr = None,
                 color: NoneStr = None):
        """Initialize BigQuery History+ Task.

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
        self.path_prefix = path_prefix or PATH_PREFIX
        super().__init__(name=name, task_type=task_type,
                         source_system=source_system,
                         source_subsystem=source_subsystem,
                         yaml_file=yaml_file,
                         sql_folder='',
                         sql_files=[],
                         env=env, thread_name=thread_name, color=color)

    def load_csv(self, dataset_id: str, skip_leading_rows: bool = 1):
        """Load CSV."""
        dataset_ref = self.bq_client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.skip_leading_rows = skip_leading_rows
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV
        uri = f'gs://{GCS_BUCKET}/' + get_path_prefix(path_prefix=self.path_prefix,
                                                      env=self.get_env(),
                                                      source_system=self.get_source_system(),
                                                      source_subsystem=self.get_source_subsystem(),
                                                      name=self.get_name(),
                                                      date_valid=self.get_date_valid(),
                                                      time_valid=self.get_time_valid()
                                                      ) + '/*'
        load_job = self.bq_client.load_table_from_uri(
            uri, dataset_ref.table(self.get_name()), job_config=job_config
        )
        load_job.result()
