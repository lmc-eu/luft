# -*- coding: utf-8 -*-
"""Column Schema."""
from luft.common.column import Column

from marshmallow import Schema, ValidationError, fields, post_load


class GDPRKeyField(fields.Field):
    """Gdpr field check."""

    def _deserialize(self, value, attr, data, **kwargs):
        if not data.get('gdpr') and value:
            raise ValidationError(
                ' Flag gdpr_key is set to True but flag gdpr is missing or False.')
        else:
            return data.get('gdpr_key')


class ColumnSchema(Schema):
    """Column schema."""

    name = fields.Str(required=True)
    type = fields.Str(attribute='data_type', required=True)
    rename = fields.Str()
    escape = fields.Boolean(default=False)
    mandatory = fields.Boolean(default=False)
    pk = fields.Boolean(default=False)
    value = fields.Str(attribute='default_value')
    gdpr = fields.Boolean(default=False)
    gdpr_key = GDPRKeyField()
    ignore = fields.Boolean(attribute='ignored', default=False)
    tech_column = fields.Boolean(default=False)
    json_path = fields.Str()

    @post_load
    def _create_column(self, data, **kwargs):
        """Create column."""
        return Column(name=data.get('name'), data_type=data.get('data_type'),
                      rename=data.get('rename'), escape=data.get('escape'),
                      mandatory=data.get('mandatory'), pk=data.get('pk'),
                      default_value=data.get('default_value'), ignored=data.get('ignored'),
                      tech_column=data.get('tech_column'))
