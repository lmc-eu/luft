# -*- coding: utf-8 -*-
"""Test Embulk JDBC task."""
from pathlib import Path

from luft.common.column import Column
from luft.common.config import BLOB_STORAGE, EMBULK_COMMAND, EMBULK_DEFAULT_TEMPLATE
from luft.tasks.embulk_jdbc_task import EmbulkJdbcTask

import pkg_resources

import pytest


@pytest.fixture(scope='function')
def embulk_jdbc_task():
    """Generate Embulk JDBC task."""
    jdbc_task = EmbulkJdbcTask(name='COUNTRYLANGUAGE',
                               task_type='embulk-jdbc-load',
                               source_system='world', source_subsystem='public',
                               columns=[
                                   Column(name='countrycode', data_type='string(3)',
                                          pk=True, mandatory=True),
                                   Column(name='language',
                                          data_type='string'),
                                   Column(
                                       name='percentage', data_type='float'),
                                   Column(
                                       name='isofficial', data_type='boolean'
                                   )
                               ]
                               )
    return jdbc_task


@pytest.mark.unit
def test_get_command(embulk_jdbc_task):
    """Test if command is ok."""
    assert embulk_jdbc_task.get_command() == EMBULK_COMMAND


def test_get_command_args(embulk_jdbc_task):
    """Test if default command args are ok."""
    task_type = embulk_jdbc_task.task_type
    embulk_template = pkg_resources.resource_filename(
        'luft', EMBULK_DEFAULT_TEMPLATE[task_type]).format(blob_storage=BLOB_STORAGE)
    assert embulk_jdbc_task.get_command_args() == [
        'run', embulk_template, '-l', 'debug']


def test_set_embulk_template(embulk_jdbc_task):
    """Test if changing of embulk template works."""
    template = pkg_resources.resource_filename(
        'luft', 'templates/embulk/jdbc_aws.yml.liquid')
    embulk_jdbc_task._set_embulk_template(template)
    template_ = Path(embulk_jdbc_task._get_embulk_template()).name
    assert template_ == 'jdbc_aws.yml.liquid'


def test_run(embulk_jdbc_task, postgres_db):
    """Tes if running of task succeed."""
    embulk_jdbc_task.__call__('2019-01-01')
