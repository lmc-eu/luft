# -*- coding: utf-8 -*-
"""Generic Task."""
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from luft.common.config import ENV
from luft.common.logger import setup_logger
from luft.common.utils import NoneStr, ts_to_tz

# Setup logger
logger = setup_logger('common', 'INFO')


class GenericTask(ABC):
    """Generic Task.

    It is used only for inheritance.
    """

    def __init__(self, name: str, task_type: str, source_system: str, source_subsystem: str,
                 yaml_file: NoneStr = None, env: NoneStr = None,
                 thread_name: NoneStr = None, color: NoneStr = None):
        """Create Generic Task.

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
        self.name = name
        self.task_type = task_type
        self.source_system = source_system
        self.source_subsystem = source_subsystem
        self.yaml_file = yaml_file
        self.env = env
        self.thread_name = thread_name
        self.color = color
        self.task_id = ''
        self.date_valid = '1970-01-01'
        self.time_valid = '0000'

    @abstractmethod
    def __call__(self, *args, **kwargs):  # pragma: no cover
        """Callable."""
        pass

    def get_env_vars(self, ts: str, env: NoneStr = None) -> Dict[str, str]:
        """Get Docker enviromental variables."""
        ts_tz = ts_to_tz(ts)
        self.set_date_valid(ts_tz.strftime('%Y-%m-%d'))
        self.set_time_valid(ts_tz.strftime('%H%M%S'))
        self.set_env(env)
        env_dict = {
            'ENV': self.get_env(),
            'TASK_TYPE': self.get_task_type(),
            'NAME': self.get_name(),
            'SOURCE_SYSTEM': self.get_source_system(),
            'SOURCE_SUBSYSTEM': self.get_source_subsystem(),
            'DATE_VALID': self.get_date_valid(),
            'TIME_VALID': self.get_time_valid(),
            'TASK_ID': self.get_task_id(),
            'THREAD_NAME': self.get_thread_name(),
            'YAML_FILE': self.get_yml_file()
        }
        clean_dict = self.clean_dictionary(env_dict)
        return clean_dict

    def set_env(self, env: NoneStr):
        """Set Task environment - PROD, DEV, etc."""
        self.env = env or ENV

    def get_env(self) -> str:
        """Get uppercased environment name.

        Default is DEV.
        """
        return self.env or ENV

    def set_date_valid(self, date_valid: str):
        """Set date valid."""
        self.date_valid = date_valid

    def get_date_valid(self) -> str:
        """Get date valid."""
        return self.date_valid

    def set_time_valid(self, time_valid: str):
        """Set time valid."""
        self.time_valid = time_valid

    def get_time_valid(self) -> str:
        """Get time valid."""
        return self.time_valid

    def set_name(self, name: str):
        """Set name."""
        self.name = name

    def get_name(self) -> str:
        """Get name."""
        return self.name

    def set_source_system(self, source_system: str):
        """Set source system."""
        self.source_system = source_system

    def get_source_system(self) -> str:
        """Get source system."""
        return self.source_system

    def set_source_subsystem(self, source_subsystem: str):
        """Set source subsystem."""
        self.source_subsystem = source_subsystem

    def get_source_subsystem(self) -> str:
        """Get source subsystem."""
        return self.source_subsystem

    def set_color(self, color: str):
        """Set hex color of Airflow operator."""
        self.color = color

    def get_color(self) -> Optional[str]:
        """Get hex color of Airflow operator."""
        return self.color

    def set_thread_name(self, thread_name: str):
        """Set thread name for Airflow."""
        self.thread_name = thread_name

    def get_thread_name(self) -> Optional[str]:
        """Get thread name for Airflow."""
        return self.thread_name

    def set_task_id(self, task_id: str):
        """Set task id."""
        self.task_id = task_id

    def get_task_id(self):
        """Get task id.

        Usable mainly for Airflow.
        """
        if self.task_id:
            return self.task_id
        return (f'{self.task_type}_{self.get_source_system().lower()}'
                f'.{self.get_source_subsystem().lower()}.{self.get_name().upper()}')

    def get_yml_file(self):
        """Get yaml file."""
        return self.yaml_file

    def get_task_type(self):
        """Get task type."""
        return self.task_type

    @staticmethod
    def clean_dictionary(env_dict: Dict[str, str]):
        """Remove none and blank items from dictionary."""
        return {k: v for k, v in env_dict.items()
                if v is not None and len(v) > 0}

    @staticmethod
    def check_mandatory(params: Dict[str, str]):
        """Check if mandatory fields are filled."""
        for key, val in params.items():
            if val is None or val == '':
                raise ValueError(f'Missing mandatory param: `{key}`.')

    @staticmethod
    def _run_subprocess(cmd: List[str], args: List[str], env: Optional[Dict[str, str]] = None):
        """Run command as subprocess.

        Parameters:
        cmd (List[str]): command to execute.
        env (Dict[str, str]): enviromental variables to set.

        """
        async def _read_output(stream, logger_instance):
            """Read output from command and print it into the right logger."""
            while True:
                line = await stream.readline()
                if line == b'':
                    break
                logger_instance(line.decode('utf-8').rstrip())

        async def _stream_subprocess(cmd, args, env):
            """Run subprocess."""
            cmd_ = ' '.join(cmd)
            args_ = ' '.join(args)
            process = await asyncio.create_subprocess_shell(f'{cmd_} {args_}',
                                                            stdout=asyncio.subprocess.PIPE,
                                                            stderr=asyncio.subprocess.PIPE,
                                                            env=env)

            await asyncio.wait([
                _read_output(process.stdout, logger.info),
                _read_output(process.stderr, logger.error)
            ])
            await process.wait()
            if process.returncode is None or process.returncode != 0:
                raise ValueError('Task failed!')

        loop = asyncio.get_event_loop()
        loop.run_until_complete(_stream_subprocess(cmd, args, env))
