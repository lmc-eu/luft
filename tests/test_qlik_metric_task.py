# -*- coding: utf-8 -*-
"""Test Qlik Metric task."""
from datetime import datetime

from luft.tasks.qlik_metric_task import QlikMetric

import pytest


@pytest.fixture(scope='function')
def qlik_metric_task():
    """Generate Qlik Metric task."""
    qlik_metric_task = QlikMetric(name='QLIK_METRIC',
                                  task_type='qlik-metric-load',
                                  source_system='qlik',
                                  source_subsystem='metric',
                                  app_id='e0c64bbd-50b3-4bc0-a404-47ae80b020a6',
                                  dimensions=[
                                      'D.Date'
                                  ],
                                  measures=[
                                      '# Applications'
                                  ],
                                  selections=[
                                      {'D.Date': [
                                          '{date_valid}']}
                                  ])
    return qlik_metric_task


@pytest.mark.unit
def test_get_selections(qlik_metric_task):
    """Test if get selection returns dictionary."""
    selection = qlik_metric_task.get_selections(datetime(2019, 9, 22))
    assert selection == {'D.Date': [43730]}


@pytest.mark.unit
def test_get_qlik_date(qlik_metric_task):
    """Test if qlik date returns right date."""
    qlik_date = qlik_metric_task.get_qlik_date(datetime(2019, 9, 18))
    assert qlik_date == 43726


@pytest.mark.unit
def test_get_templating_dates(qlik_metric_task):
    """Test if templating returns all dates."""
    qlik_temp_dates = qlik_metric_task.get_templating_dates(
        datetime(2019, 9, 22))
    assert qlik_temp_dates == {
        'date_valid': 43730,
        'week_start': 43724,
        'week_end': 43730,
        'month_start': 43709,
        'month_end': 43738
    }


@pytest.mark.integration
def test_get_measures_id_map(qlik_metric_task):
    """Test if templating returns all dates."""
    qlik_measures = qlik_metric_task.get_measures_id_map()
    assert type(qlik_measures) is dict


@pytest.mark.integration
def test_get_measures(qlik_metric_task):
    """Test if templating returns all dates."""
    qlik_measures = qlik_metric_task.get_measures()
    assert qlik_measures == [{'id': 'ufaJ', 'name': '# Applications'}]


@pytest.mark.integration
def test_get_qlik_data(qlik_metric_task):
    """Test if templating returns all dates."""
    qlik_data = qlik_metric_task.get_qlik_data(datetime(2019, 9, 22))
    assert qlik_data == [{'app_id': 'e0c64bbd-50b3-4bc0-a404-47ae80b020a6',
                          'app_name': 'Jobs.cz',
                          'app_stream_id': '63fd26e1-c55e-4931-b045-d640e1f8450f',
                          'app_stream_name': 'Product - Jobs.cz',
                          'date_valid': '1970-01-01',
                          'dimensions': {'d.date': '22. 9. 2019'},
                          'measure_id': 'ufaJ',
                          'measure_name': '# Applications',
                          'measure_value': 5067,
                          'selections': {'D.Date': [43730]}
                          }]


def test_run(qlik_metric_task):
    """Tes if running of task succeed."""
    qlik_metric_task.__call__('2019-09-22')
