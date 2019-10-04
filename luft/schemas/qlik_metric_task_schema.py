# -*- coding: utf-8 -*-
"""Qlik Metric Task Schema."""
from luft.schemas.generic_task_schema import GenericTaskSchema
from luft.tasks.qlik_metric_task import QlikMetric

from marshmallow import EXCLUDE, fields, post_load


class QlikMetricTaskSchema(GenericTaskSchema):
    """Qlik Metric Task Schema."""

    class Meta:
        """Meta."""

        unknown = EXCLUDE

    color = fields.Str(missing='#009866')
    app_id = fields.Str(required=True)
    dimensions = fields.List(fields.String())
    measures = fields.List(fields.String())
    selections = fields.List(fields.Dict(
        keys=fields.Str(), values=fields.List(fields.Str())))

    @post_load
    def make_task(self, data, **kwargs):
        """Make Qlik Metric Task."""
        return QlikMetric(name=data.get('name'), task_type=data.get('task_type'),
                          source_system=data.get('source_system'),
                          source_subsystem=data.get('source_subsystem'),
                          app_id=data.get('app_id'),
                          dimensions=data.get('dimensions'),
                          measures=data.get('measures'),
                          selections=data.get('selections'),
                          yaml_file=data.get('yaml_file'), env=data.get('env'),
                          thread_name=data.get('thread_name'), color=data.get('color')
                          )
