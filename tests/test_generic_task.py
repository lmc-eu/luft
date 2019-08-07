# -*- coding: utf-8 -*-
"""Test Generic task."""
from luft.tasks.generic_task import GenericTask

import pytest


@pytest.fixture(scope='function')
def generic_task():
    """Generate task fixture."""
    class TestClass(GenericTask):
        def __call__(self):
            pass

    return TestClass(name='OBjEcT',
                     task_type='uff',
                     source_system='AAA',
                     source_subsystem='BbbB',
                     env='KKK',
                     color='#ffffff',
                     thread_name='blabla'
                     )


@pytest.mark.unit
def test_env_init(generic_task):
    """Test if initian task ENV works."""
    assert generic_task.get_env() == 'KKK'


@pytest.mark.unit
def test_env_change(generic_task):
    """Test if changing task ENV works."""
    generic_task.set_env('DEFF')
    assert generic_task.get_env() == 'DEFF'


@pytest.mark.unit
def test_date_valid_init(generic_task):
    """Test if initial date valid works is default."""
    assert generic_task.get_date_valid() == '1970-01-01'


@pytest.mark.unit
def test_date_valid_change(generic_task):
    """Test if changing date valid works."""
    generic_task.set_date_valid('2018-01-01')
    assert generic_task.get_date_valid() == '2018-01-01'


@pytest.mark.unit
def test_time_valid_init(generic_task):
    """Test if initial time valid works is default."""
    assert generic_task.get_time_valid() == '0000'


@pytest.mark.unit
def test_time_valid_change(generic_task):
    """Test if changing time valid works."""
    generic_task.set_time_valid('0000')
    assert generic_task.get_time_valid() == '0000'


@pytest.mark.unit
def test_name_init(generic_task):
    """Test if initial name is default."""
    assert generic_task.get_name() == 'OBjEcT'


@pytest.mark.unit
def test_name_change(generic_task):
    """Test if changing name works."""
    generic_task.set_name('Test_Name')
    assert generic_task.get_name() == 'Test_Name'


@pytest.mark.unit
def test_source_system_init(generic_task):
    """Test if initial souce system is default."""
    assert generic_task.get_source_system() == 'AAA'


@pytest.mark.unit
def test_source_system_change(generic_task):
    """Test if changing source system works."""
    generic_task.set_source_system('AAAb')
    assert generic_task.get_source_system() == 'AAAb'


@pytest.mark.unit
def test_source_subsystem_init(generic_task):
    """Test if initial souce subsystem is default."""
    assert generic_task.get_source_subsystem() == 'BbbB'


@pytest.mark.unit
def test_source_subsystem_change(generic_task):
    """Test if changing source subsystem works."""
    generic_task.set_source_subsystem('FFf')
    assert generic_task.get_source_subsystem() == 'FFf'


@pytest.mark.unit
def test_color_init(generic_task):
    """Test if initial color is default."""
    assert generic_task.get_color() == '#ffffff'


@pytest.mark.unit
def test_color_change(generic_task):
    """Test if changing color works."""
    generic_task.set_color('#000')
    assert generic_task.get_color() == '#000'


@pytest.mark.unit
def test_thread_name_init(generic_task):
    """Test if initial thread name is default."""
    assert generic_task.get_thread_name() == 'blabla'


@pytest.mark.unit
def test_thread_name_change(generic_task):
    """Test if changing thread name works."""
    generic_task.set_thread_name('fff')
    assert generic_task.get_thread_name() == 'fff'


@pytest.mark.unit
def test_task_id_init(generic_task):
    """Test if initial task id is default."""
    assert generic_task.get_task_id() == 'uff_aaa.bbbb.OBJECT'


@pytest.mark.unit
def test_task_id_change(generic_task):
    """Test if changing task id works."""
    generic_task.set_task_id('Puf')
    assert generic_task.get_task_id() == 'Puf'
