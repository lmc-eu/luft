# -*- coding: utf-8 -*-
"""List of tasks."""

from pathlib import Path
from typing import Dict, List, Optional

from luft.common.config import TASK_TYPE_MAPPER
from luft.common.logger import setup_logger
from luft.common.utils import class_for_name

from ruamel import yaml

TaskListType = List[Dict[str, str]]

# Setup logger
logger = setup_logger('common', 'INFO')


class TaskList:
    """List of tasks.

    Task list is used for creating tasks from yaml file/s. Each Task is an object
    used for executing certain work.

    """

    def __init__(self):
        """Init method."""
        self.task_list: TaskListType = []

    def read_yml_string(self, yml_str: str, task_type: str = None, source_system: str = None,
                        source_subsystem: str = None, thread_cnt: int = 1,
                        blacklist: List[str] = None, whitelist: List[str] = None,
                        color: str = None, glob_filter: str = None) -> TaskListType:
        """Read yaml string and create instance of Task from it.

        Parameters:
            yml_str (str): yaml string definition.
            task_type (str): type of task. E.g. embulk-jdbc-load, mongo-load, etc.
            source_system (str): name of source system. Usually name of database.
                Used for better organization especially on blob storage. E.g. jobs, prace, pzr.
            source_subsystem (str): name of source subsystem. Usually name of schema.
                Used for better organization especially on blob storage. E.g. public, b2b.
            thread_cnt (int): number of parallel threads in Airflow. Default 1.
            blacklist (List[str]): list of tasks names that should not be executed (without suffix).
            whitelist (List[str]): list of tasks names that should be executed (without suffix).
            color (str): hex code of color. Airflow operator will have this color.
            glob_filter (str): pathname pattern for yml files. See glob library.

        Returns:
            List[Dict[str, str]]: list of tasks.

        """
        task_list_def: List[Dict[str, str]] = []
        try:
            task = yaml.load(yml_str, Loader=yaml.Loader)
        except yaml.YAMLError:
            raise
        task_list_def.append(task)
        self._process_tasks(task_list_def, whitelist, blacklist, task_type, source_system,
                            source_subsystem, thread_cnt, color)
        return self.task_list

    def read_yml_path(self, yml_path: Path, task_type: str = None, source_system: str = None,
                      source_subsystem: str = None, thread_cnt: int = 1,
                      blacklist: List[str] = None, whitelist: List[str] = None,
                      color: str = None, glob_filter: str = None) -> TaskListType:
        """Read yaml file(s) or yaml definition and create instances of Tasks from it.

        In case of folder, it reads all yaml files in directory, concatenates them and then creates
        instances of all tasks.

        Parameters:
            yml_path (Path): folder with yml files or yml file
            task_type (str): type of task. E.g. embulk-jdbc-load, mongo-load, etc.
            source_system (str): name of source system. Usually name of database.
                Used for better organization especially on blob storage. E.g. jobs, prace, pzr.
            source_subsystem (str): name of source subsystem. Usually name of schema.
                Used for better organization especially on blob storage. E.g. public, b2b.
            thread_cnt (int): number of parallel threads in Airflow. Default 1.
            blacklist (List[str]): list of tasks names that should not be executed (without suffix).
            whitelist (List[str]): list of tasks names that should be executed (without suffix).
            color (str): hex code of color. Airflow operator will have this color.
            glob_filter (str): pathname pattern for yml files. See glob library.

        Returns:
            List[Dict[str, str]]: list of tasks.

        """
        task_list_def: List[Dict[str, str]] = []
        try:
            if yml_path.is_dir():  # yaml var is a directory
                glob_filter = '**/*.yml' if not glob_filter else glob_filter
                yml_files = sorted(
                    list(yml_path.glob(glob_filter)), reverse=True)
                for yml_file in yml_files:
                    task = yaml.load(yml_file.read_text(), Loader=yaml.Loader)
                    self._add_yaml_file_loc(task, yml_file)
                    task_list_def.append(task)
            elif yml_path.is_file():  # yaml var is a file
                task = yaml.load(yml_path.read_text(), Loader=yaml.Loader)
                if isinstance(task, list):  # if yaml contains multiple definitions
                    self._separate_tasks(task, yml_path, task_list_def)
                else:
                    self._add_yaml_file_loc(task, yml_file)
                    task_list_def.append(task)
            else:
                raise TypeError(
                    'Unknown format of yaml or non-existent folder.')
        except yaml.YAMLError:
            raise
        self._process_tasks(task_list_def, whitelist, blacklist, task_type, source_system,
                            source_subsystem, thread_cnt, color)
        return self.task_list

    def _process_tasks(self, task_list_def: TaskListType, whitelist: Optional[List[str]],
                       blacklist: Optional[List[str]], task_type: Optional[str],
                       source_system: Optional[str], source_subsystem: Optional[str],
                       thread_cnt: Optional[int], color: Optional[str]):
        """Take tasks definition and generate task list from them."""
        task_list_def = self._filter_task_list(
            task_list_def, whitelist, blacklist)
        self._add_task_id(task_list_def)  # add unique id to every task
        # Get schema class
        schema_class = class_for_name(TASK_TYPE_MAPPER.get(task_type))
        context = self._get_context(
            task_type, source_system, source_subsystem, thread_cnt, color)
        self.task_list = schema_class(
            many=True, context=context).load(task_list_def)

    def _separate_tasks(self, tasks: TaskListType, yml_path: Path, task_list_def: TaskListType):
        """If one yaml file contains multiple objects/tasks, separate them."""
        # more tasks in one file
        for task in tasks:
            self._add_yaml_file_loc(task, yml_path)
            task_list_def.append(task)

    @staticmethod
    def _add_task_id(task_list: TaskListType):
        """Add unique task_id to every task."""
        for c, task in enumerate(task_list, 1):
            task['id'] = str(c)

    @staticmethod
    def _add_yaml_file_loc(task: Dict[str, str], yml_path: Path):
        """Append yaml_file info to task definition."""
        if yml_path.is_file():
            task['yaml_file'] = str(yml_path)

    @staticmethod
    def _filter_task_list(task_list: TaskListType, whitelist: Optional[List[str]],
                          blacklist: Optional[List[str]]) -> TaskListType:
        """Filter list of task by blacklist or whitelist."""
        if whitelist and len(whitelist) > 0:  # whitelist
            result = [task for task in task_list if task.get(
                'name') in whitelist]
            return result
        if blacklist and len(blacklist) > 0:  # blacklist
            result = [task for task in task_list if task.get(
                'name') not in blacklist]
            return result
        return task_list  # if whitelist and blacklist is null

    @staticmethod
    def _get_context(task_type: Optional[str], source_system: Optional[str],
                     source_subsystem: Optional[str], thread_cnt: Optional[int],
                     color: Optional[str]) -> Dict[str, Optional[str]]:
        """Get context information that is same for all tasks."""
        context = {'task_type': task_type,
                   'source_system': source_system,
                   'source_subsystem': source_subsystem,
                   'thread_count': str(thread_cnt),
                   'color': color}
        return context
