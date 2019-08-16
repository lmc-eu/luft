# -*- coding: utf-8 -*-
"""Luft cli."""
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Union

import click

from luft.common.config import TASKS_FOLDER
from luft.common.task_list import TaskList

task_list_options = [
    click.option('--yml-path', '-y',
                 prompt='Path or single YML file path inside default task folder',
                 required=True, help='Path or YML file inside default task folder'),
    click.option('--start-date', '-s',
                 help='Start date in format YYYY-MM-DD for looping.'),
    click.option('--start-time', '-t',
                 help='Start time in format HHMMSS.'),
    click.option('--end-date', '-e',
                 help='End date in format YYYY-MM-DD for looping.'),
    click.option('--source-system', '-sys', help='Name of source system (usually same as name of DB'
                 ' or subfolder of default task folder). If not provided the source system is taken'
                 ' from subfolder name. E.g. --yml-path gis (thus ./dags/tasks/gis) ; if source'
                 ' system is not provided, it will take `gis` name from folder structure.'),
    click.option('--source-subsystem', '-sub', help='Name of source subsystem (usually same as name'
                 ' of schema or subfolder of default task/<source_system>). If not provided'
                 ' the source system is taken from subfolder name. E.g. --yml-path gis (thus'
                 ' ./dags/tasks/gis/public); if source system is not provided, it will take'
                 ' `public` name from folder structure.'),
    click.option('--blacklist', '-b', multiple=True, help='Name of tables/objects to be ignored'
                 ' during processing. E.g. --yml-path gis and -b TEST. It will process all tables'
                 ' /objects under ./dags/tasks/gis folder except object TEST.'),
    click.option('--whitelist', '-w', multiple=True, help='Name of tables/objects to be only'
                 ' processed.E.g. --yml-path gis and -w TEST. It will process only table'
                 '/objects under ./dags/tasks/gis that has name TEST.'),
    click.option('--glob-filter', '-g'),
]


def add_options(options):
    """Add option to click function."""
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


def _daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        date_valid = start_date + timedelta(n)
        yield date_valid.strftime('%Y-%m-%d')


def _loop_tasks(task_list, **kwargs):
    start = datetime.strptime(kwargs.get('start_date'), '%Y-%m-%d') if kwargs.get('start_date') \
        else date.today() - timedelta(1)
    end = datetime.strptime(kwargs.get('end_date'), '%Y-%m-%d') if kwargs.get('end_date') \
        else start + timedelta(days=1)
    for date_valid in _daterange(start, end):
        for task in task_list:
            task(ts=date_valid, **kwargs)
            click.secho(f'Task `{task.get_task_id()}` is done!', fg='green')


def _create_tasks(task_type: str, yml_path: str, source_system: Optional[str],
                  source_subsystem: Optional[str], blacklist: Optional[List[str]],
                  whitelist: Optional[List[str]], glob_filter: Optional[str]):
    """Create task list."""
    yml_path = TASKS_FOLDER / yml_path
    task_list = TaskList().read_yml_path(
        yml_path=yml_path,
        task_type=task_type,
        source_system=source_system,
        source_subsystem=source_subsystem,
        blacklist=blacklist,
        whitelist=whitelist,
        glob_filter=glob_filter
    )
    return task_list


@click.group()
@click.pass_context
def luft(_ctx: click.core.Context):
    """Luft client."""
    pass


@luft.group(help='Tools for working with JDBC sources.')
@click.pass_context
def jdbc(_ctx: click.core.Context):
    """Tools for working with JDBC sources."""
    pass


@jdbc.command(help='Load data from jdbc source into blob storage.')
@add_options(task_list_options)
@click.pass_context
def load(ctx: click.core.Context, yml_path: str, start_date: str, start_time: str,
         end_date: str, source_system: str, source_subsystem: str, blacklist: List[str],
         whitelist: List[str], glob_filter: str):
    """Load data from jdbc source into blob storage."""
    task_list = _create_tasks(task_type='embulk-jdbc-load', yml_path=yml_path,
                              source_system=source_system, source_subsystem=source_subsystem,
                              blacklist=blacklist, whitelist=whitelist, glob_filter=glob_filter)
    _loop_tasks(task_list)


@luft.group(help='Tools for working with BigQuery.')
@click.pass_context
def bq(_ctx):
    """BigQuery."""
    pass


@bq.command(help='Execute commands in BigQuery.')
@add_options(task_list_options)
@click.option('--script-blacklist', '-sb', multiple=True)
@click.option('--script-whitelist', '-sw', multiple=True)
@click.pass_context
def exec(ctx: click.core.Context, yml_path: str, start_date: str, start_time: str,
         end_date: str, source_system: str, source_subsystem: str, blacklist: List[str],
         whitelist: List[str], glob_filter: str, script_whitelist: Union[List[str], None],
         script_blacklist: Union[List[str], None]):
    """Execute commands in BigQuery."""
    task_list = _create_tasks(task_type='bq-exec', yml_path=yml_path,
                              source_system=source_system, source_subsystem=source_subsystem,
                              blacklist=blacklist, whitelist=whitelist, glob_filter=glob_filter)
    task_list = filter_script_list(
        task_list, script_whitelist, script_blacklist)
    _loop_tasks(task_list)


@luft.group(help='Tools for working with Qlik Sense Cloud.')
@click.pass_context
def qlik_cloud(_ctx):
    """Qlik Cloud."""
    pass


@qlik_cloud.command(help='Export app from QSE, upload it and publish it into Qlik Sense Cloud.')
@add_options(task_list_options)
@click.pass_context
def upload(ctx: click.core.Context, yml_path: str, start_date: str, start_time: str,
           end_date: str, source_system: str, source_subsystem: str, blacklist: List[str],
           whitelist: List[str], glob_filter: str):
    """Upload app to Qlik Sense Cloud."""
    task_list = _create_tasks(task_type='qlik-cloud-upload', yml_path=yml_path,
                              source_system=source_system, source_subsystem=source_subsystem,
                              blacklist=blacklist, whitelist=whitelist, glob_filter=glob_filter)
    _loop_tasks(task_list)


def filter_script_list(task_list, whitelist, blacklist):
    """Filter list of script."""
    if whitelist and len(whitelist) > 0:
        for task in task_list:
            task.sql_files = [
                file for file in task.sql_files if Path(file).stem in whitelist]
    if blacklist and len(blacklist) > 0:
        for task in task_list:
            task.sql_files = [file for file in task.sql_files if Path(
                file).stem not in blacklist]
    return task_list


if __name__ == '__main__':
    # load(['-y', 'world'])
    luft(obj={})
