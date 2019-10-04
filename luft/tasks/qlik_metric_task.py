# -*- coding: utf-8 -*-
"""Qlik Metric Task."""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from luft.common.config import (AWS_ACCESS_KEY_ID, AWS_BUCKET, AWS_SECRET_ACCESS_KEY,
                                QLIK_ENT_CLIENT_CERT, QLIK_ENT_CLIENT_KEY, QLIK_ENT_HOST,
                                QLIK_ENT_ROOT_CERT)
from luft.common.logger import setup_logger
from luft.common.s3_utils import get_s3, write_s3
from luft.common.utils import NoneStr, ts_to_tz
from luft.tasks.generic_task import GenericTask
from luft.vendor.pyqlikengine import engine_helper, pyqlikengine

# Setup logger
logger = setup_logger('common', 'INFO')


class QlikMetric(GenericTask):
    """Qlik Sense export metrics."""

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 app_id: str, dimensions: Union[List[str]] = None,
                 measures: Union[List[str]] = None,
                 selections: Union[List[Dict[str, List[str]]]] = None,
                 yaml_file: NoneStr = None, env: NoneStr = None,
                 thread_name: NoneStr = None, color: NoneStr = None):
        """Initialize BigQuery JDBC Task.

        Attributes:
        name (str): name of task.
        task_type (str): type of task. E.g. embulk-jdbc-load, mongo-load, etc.
        source_system (str): name of source system. Usually name of database.
            Used for better organization especially on blob storage. E.g. jobs, prace, pzr.
        source_subsystem (str): name of source subsystem. Usually name of schema.
            Used for better organization especially on blob storage. E.g. public, b2b.
        account_id (str): Qlik sense cloud account id.
        apps List[Dict[str, str]]: List of apps parameters.
        yaml_file (str): yaml filepath.
        env (str): environment - PROD, DEV.
        thread_name (str): name of thread for Airflow parallelization.
        color (str): hex code of color. Airflow operator will have this color.

        """
        self.engine = self.qlik_login()
        self.app_id = app_id
        self.dimensions = dimensions
        self.measures = measures
        self.selections = selections
        self.date_valid = None

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
        ts_tz = ts_to_tz(ts)
        self.date_valid = ts_tz.strftime('%Y-%m-%d')
        qlik_data = self.get_qlik_data(ts_tz=ts_tz)
        self.write_blob_storage(json_list=qlik_data)

    def qlik_login(self):
        """Login to Qlik Sense."""
        return pyqlikengine.QixEngine(url=QLIK_ENT_HOST, user_directory='LMC',
                                      user_id='tomsejr', ca_certs=QLIK_ENT_ROOT_CERT,
                                      certfile=QLIK_ENT_CLIENT_CERT,
                                      keyfile=QLIK_ENT_CLIENT_KEY)

    def get_selections(self, ts_tz: datetime) -> Dict[str, Any]:
        """Get Selection from Qlik Application.

        Attributes:
        ts(str): date of valid
        selections(List[Dict[str, str]]): selection definitions.

        """
        result: Dict[str, Any] = {}
        template_dates = self.get_templating_dates(ts_tz)
        if self.selections:
            for selection in self.selections:
                for key, values in selection.items():
                    val_list: List[Union[str, int]] = []
                    for value in values:
                        value = value.format_map(template_dates)
                        try:
                            val_list.append(int(value))
                        except ValueError:
                            val_list.append(value)
                    result[key] = val_list
        return result

    def get_qlik_date(self, date_obj):
        """Get Qlik Sense representation of date.

        Arguments:
        date_obj(datetime): datetime object to transform.

        """
        qlik_start_date = datetime(year=1899, month=12, day=31)
        return abs(date_obj.replace(tzinfo=None) - qlik_start_date).days + 1

    def get_templating_dates(self, ts_tz: datetime):
        """Get list of templating dates.

        Arguments:
        ts(str): string representing date.

        """
        week_start = ts_tz - timedelta(days=(ts_tz.weekday()))
        week_end = week_start + timedelta(days=6)
        month_start = ts_tz.replace(day=1)
        next_month = ts_tz.replace(day=28) + timedelta(days=4)
        month_end = next_month - timedelta(days=next_month.day)
        return {
            'date_valid': self.get_qlik_date(ts_tz),
            'week_start': self.get_qlik_date(week_start),
            'week_end': self.get_qlik_date(week_end),
            'month_start': self.get_qlik_date(month_start),
            'month_end': self.get_qlik_date(month_end)
        }

    def get_measures_id_map(self):
        """Get list of all app master measures and its IDs."""
        result = {}
        params = {
            'qInfo': {
                'qType': 'MeasureList'
            },
            'qMeasureListDef': {
                'qType': 'measure',
                'qData': {}
            }
        }
        session_obj = self.engine.eaa.create_session_object(
            self.engine.app_handle, params)
        app_layout = self.engine.egoa.get_layout(
            session_obj.get('qReturn').get('qHandle'))
        for item in app_layout.get('qLayout').get('qMeasureList').get('qItems'):
            key = item.get('qMeta').get('title')
            value = item.get('qInfo').get('qId')
            if key and value:
                result[key.lower()] = {'name': key, 'id': value}
        return result

    def get_measures(self):
        """Get list of master measures."""
        result = []
        measure_id_map = self.get_measures_id_map()
        if self.measures:
            for measure in self.measures:
                measure_map = measure_id_map.get(measure.lower())
                if measure_map:
                    result.append(measure_map)
        else:
            for _key, value in measure_id_map.items():
                result.append(value)
        return result

    def write_blob_storage(self, json_list):
        """Write json list to blob storage."""
        # Todo Implement GCS
        logger.info(f'Writing result to S3.')
        result = ''
        for item in json_list:
            item_tmp = json.dumps(item)
            result += str(item_tmp) + '\n'
        s3 = get_s3(aws_access_key=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        write_s3(
            env=self.env,
            source_system=self.source_system,
            source_subsystem=self.source_subsystem,
            object_name=self.name,
            s3=s3,
            s3_bucket=AWS_BUCKET,
            content=result,
            date_valid=self.date_valid
        )

    def get_qlik_data(self, ts_tz: datetime):
        """Get Qlik data from hypercube.

        Arguments:
        ts(str): time and date.
        """
        # Open application
        logger.info(f'Opening app: {self.app_id}')
        app = self.engine.open_app(self.app_id)
        handle = self.engine.ega.get_handle(app)
        measure_dict = self.get_measures()
        logger.info(f'Getting measures: {measure_dict}')
        conn = self.engine.get_connection()
        select_dict = self.get_selections(ts_tz)
        logger.info(f'Selecting_values: {select_dict}')
        logger.info(f'Creating hypercube.')
        qlik_data = engine_helper.get_hypercube_data(
            conn, handle, measure_dict, self.dimensions, select_dict, self.date_valid)
        self.engine.disconnect()
        logger.info(f'Engine disconnected.')
        return qlik_data
