# -*- coding: utf-8 -*-
"""Generic Task Schema."""
from luft.common.config import TASK_TYPE_MAPPER, THREAD_NAME_PREFIX
from luft.common.utils import get_source_subsystem, get_source_system

from marshmallow import EXCLUDE, Schema, ValidationError, fields, pre_load, validates


class GenericTaskSchema(Schema):
    """Generic Task Schema validation."""

    class Meta:
        """Whether to exclude, include, or raise an error for unknown fields in the data."""

        unknown = EXCLUDE

    # Mandatory fields
    name = fields.Str(required=True)
    task_type = fields.Str(required=True)
    source_system = fields.Str(required=True)
    source_subsystem = fields.Str(required=True)
    # Non-mandatory fields
    id = fields.Int()
    yaml_file = fields.Str()
    env = fields.Str()
    thread_name = fields.Str()
    color = fields.Str()

    @pre_load
    def _prepare_task(self, data, **kwargs):
        """Set variables before running validation."""
        # Task_type
        if not data.get('task_type'):  # if task_type is not specified in yaml file
            if self.context.get('task_type'):  # take task type from context
                data['task_type'] = self.context.get('task_type')
            else:
                raise ValidationError(
                    'You did not specified task_type in yml file or in context.')
        # Source system
        if not data.get('source_system'):  # if source system is not specified in yaml file
            # take source system from context or from source folder
            data['source_system'] = get_source_system(
                data.get('yaml_file'), self.context.get('source_system'))
        # Source subsystem
        if not data.get('source_subsystem'):  # if src sub is not specified in yaml file
            # take source subsystem from context or from source folde
            data['source_subsystem'] = get_source_subsystem(
                data.get('yaml_file'), self.context.get('source_subsystem'))
        # if there is id (position of task in task list) and thread count is specified
        # and thread_name is not specified it yaml file. Take ID and do `ID mod thread_count`
        # and task is then pushed to thread with re result of this formula.
        if data.get('id') and self.context.get('thread_count') and not data.get('thread_name'):
            data['thread_name'] = THREAD_NAME_PREFIX + \
                str(int(data.get('id')) %
                    int(self.context.get('thread_count')))
        return data

    @validates('task_type')
    def _validates_type(self, data):
        """Check that task type is from list of known tasks."""
        if not TASK_TYPE_MAPPER.get(data):
            raise ValidationError(
                f'Type of task {data} is not supported. Please change it!')
