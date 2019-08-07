# -*- coding: utf-8 -*-
"""Test BQ exec task."""
from luft.tasks.bq_exec_task import BQExecTask

import pytest


@pytest.fixture(scope='function')
def bq_exec_task():
    """Generate Embulk JDBC task."""
    jdbc_task = BQExecTask(name='Test',
                           task_type='bq-exec',
                           source_system='bq', source_subsystem='exec',
                           sql_folder='example/sql/', sql_files=['bq.sql']
                           )
    return jdbc_task


def test_run(bq_exec_task):
    """Test if running of task succeed."""
    bq_exec_task.__call__('2019-01-01')
