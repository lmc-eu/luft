# -*- coding: utf-8 -*-
"""BigQuery Exec Task Schema.."""
from luft.schemas.generic_task_schema import GenericTaskSchema
from luft.tasks.qlik_cloud_task import QlikCloud

from marshmallow import Schema, fields, post_load


class QlikAppsSchema(Schema):
    """Qlik Apps Schema."""

    name = fields.Str(required=True)
    filename = fields.Str(required=True)
    qse_id = fields.Str(required=True)
    qsc_stream = fields.Str(required=True)


class QlikCloudTaskSchema(GenericTaskSchema):
    """BigQuery Exec Task Schema."""

    color = fields.Str(missing='#009845')
    account_id = fields.Str(required=True)
    apps = fields.Nested(QlikAppsSchema, many=True)

    @post_load
    def make_task(self, data, **kwargs):
        """Make BigQuery Exec Task."""
        return QlikCloud(name=data.get('name'), task_type=data.get('task_type'),
                         source_system=data.get('source_system'),
                         source_subsystem=data.get('source_subsystem'),
                         account_id=data.get('account_id'),
                         apps=data.get('apps'),
                         yaml_file=data.get('yaml_file'), env=data.get('env'),
                         thread_name=data.get('thread_name'), color=data.get('color')
                         )
