# -*- coding: utf-8 -*-
"""Embulk JDBC Task Schema."""
from luft.schemas.column_schema import ColumnSchema
from luft.schemas.generic_task_schema import GenericTaskSchema
from luft.tasks.embulk_jdbc_task import EmbulkJdbcTask

from marshmallow import fields, post_load


class EmbulkJdbcTaskSchema(GenericTaskSchema):
    """Embulk Jdbc schema."""

    color = fields.Str(missing='#A3E9DA')
    historize = fields.Str()
    columns = fields.Nested(ColumnSchema, many=True)
    embulk_template = fields.String()
    fetch_rows = fields.Int()
    source_table = fields.Str()
    where_clause = fields.Str()

    @post_load
    def _make_task(self, data, **kwargs):
        """Make Embulk JDBC Task."""
        return EmbulkJdbcTask(name=data.get('name'), task_type=data.get('task_type'),
                              source_system=data.get('source_system'),
                              source_subsystem=data.get('source_subsystem'),
                              columns=data.get('columns'), historize=data.get('historize'),
                              fetch_rows=data.get('fetch_rows'),
                              source_table=data.get('source_table'),
                              where_clause=data.get('where_clause'),
                              embulk_template=data.get('embulk_template'),
                              yaml_file=data.get('yaml_file'),
                              env=data.get('env'), thread_name=data.get('thread_name'),
                              color=data.get('color'))
