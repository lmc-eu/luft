# -*- coding: utf-8 -*-
"""Embulk JDBC Task Schema."""
from luft.schemas.column_schema import ColumnSchema
from luft.schemas.generic_task_schema import GenericTaskSchema
from luft.tasks.embulk_es_task import EmbulkEsTask

from marshmallow import fields, post_load


class EmbulkEsTaskSchema(GenericTaskSchema):
    """Embulk Jdbc schema."""

    color = fields.Str(missing='#A3E9DA')
    columns = fields.Nested(ColumnSchema, many=True)
    embulk_template = fields.String()
    index_name = fields.Str()
    type_name = fields.Str()
    queries_clause = fields.Str()
    path_prefix = fields.Str()

    @post_load
    def _make_task(self, data, **kwargs):
        """Make Embulk Elasticsearch Task."""
        return EmbulkEsTask(name=data.get('name'),
                              task_type=data.get('task_type'),
                              source_system=data.get('source_system'),
                              source_subsystem=data.get('source_subsystem'),
                              columns=data.get('columns'),
                              index_name=data.get('index_name'),
                              type_name=data.get('type_name'),
                              embulk_template=data.get('embulk_template'),
                              queries_clause=data.get('queries_clause'),
                              yaml_file=data.get('yaml_file'),
                              path_prefix=data.get('path_prefix'),
                              env=data.get('env'),
                              thread_name=data.get('thread_name'),
                              color=data.get('color'))
