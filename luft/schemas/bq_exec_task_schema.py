# -*- coding: utf-8 -*-
"""BigQuery Exec Task Schema.."""
from luft.schemas.generic_task_schema import GenericTaskSchema
from luft.tasks.bq_exec_task import BQExecTask

from marshmallow import fields, post_load


class BQExecTaskSchema(GenericTaskSchema):
    """BigQuery Exec Task Schema."""

    color = fields.Str(missing='#73DBF5')
    sql_folder = fields.Str(required=True)
    sql_files = fields.List(fields.Str(), required=True)
    project_id = fields.Str()
    location = fields.Str()

    @post_load
    def make_task(self, data, **kwargs):
        """Make BigQuery Exec Task."""
        return BQExecTask(name=data.get('name'), task_type=data.get('task_type'),
                          source_system=data.get('source_system'),
                          source_subsystem=data.get('source_subsystem'),
                          sql_folder=data.get('sql_folder'),
                          sql_files=data.get('sql_files'),
                          project_id=data.get('project_id'),
                          location=data.get('location'),
                          yaml_file=data.get('yaml_file'), env=data.get('env'),
                          thread_name=data.get('thread_name'), color=data.get('color')
                          )
