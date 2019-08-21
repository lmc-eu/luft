# -*- coding: utf-8 -*-
"""BigQuery Load Task Schema."""
from luft.schemas.column_schema import ColumnSchema
from luft.schemas.generic_task_schema import GenericTaskSchema
from luft.tasks.bq_load_task import BQLoadTask

from marshmallow import fields, post_load


class BQLoadTaskSchema(GenericTaskSchema):
    """BigQuery Load Task Schema."""

    color = fields.Str(missing='#03A0F3')
    columns = fields.Nested(ColumnSchema, many=True)
    project_id = fields.Str()
    location = fields.Str()
    dataset_id = fields.Str()
    skip_leading_rows = fields.Boolean(missing=True)
    field_delimiter = fields.Str(missing='\t')
    disable_check = fields.Boolean(missing=False)
    path_prefix = fields.Str()

    @post_load
    def make_task(self, data, **kwargs):
        """Make BigQuery Load Task."""
        return BQLoadTask(name=data.get('name'), task_type=data.get('task_type'),
                          source_system=data.get('source_system'),
                          source_subsystem=data.get('source_subsystem'),
                          columns=data.get('columns'),
                          project_id=data.get('project_id'),
                          location=data.get('location'),
                          dataset_id=data.get('dataset_id'),
                          skip_leading_rows=data.get('skip_leading_rows'),
                          disable_check=data.get('disable_check'),
                          field_delimiter=data.get('field_delimiter'),
                          path_prefix=data.get('path_prefix'),
                          yaml_file=data.get('yaml_file'), env=data.get('env'),
                          thread_name=data.get('thread_name'), color=data.get('color')
                          )
