# -*- coding: utf-8 -*-
"""Test Generic task."""

from luft.common.column import Column
from luft.schemas.column_schema import ColumnSchema

import pytest


@pytest.fixture(scope='function')
def columns():
    """Column fixture."""
    cols = [
        {  # 0 PK
            'name': 'Pk1',
            'rename': 'Primary_Key1',
            'type': 'string(32)',
            'mandatory': True,
            'pk': True
        },
        {  # 1 PK
            'name': 'Pk2',
            'rename': 'Primary_Key2',
            'type': 'string(32)',
            'mandatory': True,
            'pk': True
        },
        {  # 2 Escaped column
            'name': 'Esc',
            'rename': 'Escaped',
            'type': 'number(32)',
            'escape': True
        },
        {  # 3 Default value
            'name': 'Defl',
            'rename': 'Default',
            'type': 'number(32)',
            'value': '23'
        },
        {  # 4 GDPR
            'name': 'gdpr',
            'type': 'number(32)'
        },
        {  # 5 Ignored - PK
            'name': 'ignor_pk',
            'type': 'number(32)',
            'pk': True,
            'ignore': True
        },
        {  # 6 Ignored - NonPK
            'name': 'ignor_nonPk',
            'rename': 'Ignored_NONPK',
            'type': 'number(32)',
            'ignore': True
        },
        {  # 7 Tech column - PK
            'name': 'tech_PK',
            'rename': 'Technical_PK',
            'type': 'number(32)',
            'pk': True,
            'tech_column': True
        },
        {  # 8 Tech column - nonPK
            'name': 'tech_nonPK',
            'rename': 'Technical_NONPK',
            'type': 'number(32)',
            'tech_column': True
        },
        {  # 9 Tech ignored
            'name': 'tech_ign',
            'type': 'number(32)',
            'tech_column': True,
            'ignore': True
        },
        {  # 10 DW_GDPR_FLAG
            'name': 'DW_GDPR_FLAG',
            'type': 'varchar(1)',
            'tech_column': True
        },
        {  # 11 JSON
            'name': 'JSON',
            'type': 'variant'
        }
    ]
    columns = ColumnSchema(many=True).load(cols)
    #  add bad type column
    columns.append(Column(
        name='bad_type_column',
        data_type='some_bullshit(23)'
    ))
    i = 1
    for column in columns:
        column.set_index(i)
        i += 1
    return columns


@pytest.mark.unit
def test_base_columns(columns):
    """Test that columns has trully values."""
    assert columns[0].name == 'Pk1'
    assert columns[0].rename == 'Primary_Key1'
    assert columns[0].data_type == 'string(32)'
    assert columns[0].pk is True
    assert columns[1].name == 'Pk2'
    assert columns[1].pk is True
    assert columns[2].escape is True
    assert columns[8].tech_column is True
    assert columns[9].ignored is True


@pytest.mark.unit
def test_should_return_pk(columns):
    """Test behavior of pk columns."""
    assert columns[0]._should_return(col_type='pk') is True
    assert columns[0]._should_return(col_type='nonpk') is False
    assert columns[0]._should_return(col_type='all') is True
    assert columns[0]._should_return(
        col_type='pk', filter_ignored=True) is True
    assert columns[0]._should_return(
        col_type='pk', filter_ignored=False) is True
    assert columns[0]._should_return(col_type='pk', include_tech=True) is True
    assert columns[0]._should_return(col_type='pk', include_tech=False) is True


@pytest.mark.unit
def test_should_return_nonpk(columns):
    """Test behavior of nonpk columns."""
    assert columns[2]._should_return(col_type='pk') is False
    assert columns[2]._should_return(col_type='nonpk') is True
    assert columns[2]._should_return(col_type='all') is True
    assert columns[2]._should_return(
        col_type='nonpk', filter_ignored=True) is True
    assert columns[2]._should_return(
        col_type='nonpk', filter_ignored=False) is True
    assert columns[2]._should_return(
        col_type='nonpk', include_tech=True) is True
    assert columns[2]._should_return(
        col_type='nonpk', include_tech=False) is True


@pytest.mark.unit
def test_should_return_ignor_pk(columns):
    """Test behavior of ignored pk columns."""
    assert columns[5]._should_return(col_type='pk') is False
    assert columns[5]._should_return(col_type='nonpk') is False
    assert columns[5]._should_return(col_type='all') is False
    assert columns[5]._should_return(
        col_type='pk', filter_ignored=True) is False
    assert columns[5]._should_return(
        col_type='pk', filter_ignored=False) is True
    assert columns[5]._should_return(col_type='pk', include_tech=True) is False
    assert columns[5]._should_return(
        col_type='pk', include_tech=False) is False


@pytest.mark.unit
def test_should_return_ignor_nonpk(columns):
    """Test behavior of ignored nonpk columns."""
    assert columns[6]._should_return(col_type='pk') is False
    assert columns[6]._should_return(col_type='nonpk') is False
    assert columns[6]._should_return(col_type='all') is False
    assert columns[6]._should_return(
        col_type='nonpk', filter_ignored=True) is False
    assert columns[6]._should_return(
        col_type='nonpk', filter_ignored=False) is True
    assert columns[6]._should_return(
        col_type='nonpk', include_tech=True) is False
    assert columns[6]._should_return(
        col_type='nonpk', include_tech=False) is False


@pytest.mark.unit
def test_should_return_tech_pk(columns):
    """Test behavior of technical pk columns."""
    assert columns[7]._should_return(col_type='pk') is True
    assert columns[7]._should_return(col_type='nonpk') is False
    assert columns[7]._should_return(col_type='all') is True
    assert columns[7]._should_return(
        col_type='pk', filter_ignored=True) is True
    assert columns[7]._should_return(
        col_type='pk', filter_ignored=False) is True
    assert columns[7]._should_return(col_type='pk', include_tech=True) is True
    assert columns[7]._should_return(
        col_type='pk', include_tech=False) is False
    assert columns[7]._should_return(
        col_type='nonpk', include_tech=False) is False


@pytest.mark.unit
def test_should_return_tech_nonpk(columns):
    """Test behavior of technical nonpk columns."""
    assert columns[8]._should_return(col_type='pk') is False
    assert columns[8]._should_return(col_type='nonpk') is True
    assert columns[8]._should_return(col_type='all') is True
    assert columns[8]._should_return(
        col_type='nonpk', filter_ignored=True) is True
    assert columns[8]._should_return(
        col_type='nonpk', filter_ignored=False) is True
    assert columns[8]._should_return(
        col_type='nonpk', include_tech=True) is True
    assert columns[8]._should_return(
        col_type='nonpk', include_tech=False) is False
    assert columns[8]._should_return(
        col_type='pk', include_tech=False) is False


@pytest.mark.unit
def test_should_return_ignor_tech_nonpk(columns):
    """Test behavior of ignored and technical nonpk columns."""
    assert columns[9]._should_return(col_type='pk') is False
    assert columns[9]._should_return(col_type='nonpk') is False
    assert columns[9]._should_return(col_type='all') is False
    assert columns[9]._should_return(
        col_type='nonpk', filter_ignored=True) is False
    assert columns[9]._should_return(
        col_type='nonpk', filter_ignored=False) is True
    assert columns[9]._should_return(
        col_type='nonpk', include_tech=True) is False
    assert columns[9]._should_return(
        col_type='nonpk', include_tech=False) is False
    assert columns[9]._should_return(
        col_type='pk', include_tech=False) is False
    assert columns[9]._should_return(
        col_type='nonpk', filter_ignored=True, include_tech=True) is False
    assert columns[9]._should_return(
        col_type='nonpk', filter_ignored=False, include_tech=True) is True
    assert columns[9]._should_return(
        col_type='nonpk', filter_ignored=True, include_tech=False) is False
    assert columns[9]._should_return(
        col_type='nonpk', filter_ignored=False, include_tech=False) is False


@pytest.mark.unit
def test_get_index(columns):
    """Test that indexing for Snowflake load is ok."""
    assert columns[0].get_index() == '$1 AS PRIMARY_KEY1'
    assert columns[4].get_index() == '$5 AS GDPR'
    assert columns[9].get_index() is None


@pytest.mark.unit
def test_get_name(columns):
    """Column name is uppercased name or rename."""
    assert columns[1].get_name() == 'PRIMARY_KEY2'
    assert columns[5].get_name() is None


@pytest.mark.unit
def test_get_aliased_name(columns):
    """Aliased is name or value AS rename or name."""
    assert columns[1].get_aliased_name() == 'PK2 AS PRIMARY_KEY2'
    assert columns[3].get_aliased_name() == '23 AS DEFAULT'
    assert columns[5].get_aliased_name() is None


@pytest.mark.unit
def test_get_def(columns):
    """Definition is only valid and has mandatory suffix."""
    assert columns[1].get_def() == 'PRIMARY_KEY2 STRING(32) NOT NULL'
    assert columns[3].get_def() == 'DEFAULT NUMBER(32)'
    assert columns[4].get_def() == 'GDPR NUMBER(32)'
    with pytest.raises(TypeError):
        assert columns[-1].get_def()


@pytest.mark.unit
def test_get_coalesce(columns):
    """Coalesce is different for pk and non pk columns."""
    assert columns[1].get_coalesce(
    ) == 'COALESCE(s.PRIMARY_KEY2, t.PRIMARY_KEY2) AS PRIMARY_KEY2'
    assert columns[3].get_coalesce() == 'CASE WHEN s.DW_LOAD_DATE IS NOT NULL THEN s.DEFAULT ' \
                                        'ELSE t.DEFAULT END AS DEFAULT'
    assert columns[4].get_coalesce(
    ) == 'CASE WHEN s.DW_LOAD_DATE IS NOT NULL THEN s.GDPR ELSE t.GDPR END AS GDPR'


@pytest.mark.unit
def test_get_join(columns):
    """Test that joining condition is always with rename."""
    assert columns[0].get_join() == 's.PRIMARY_KEY1 = t.PRIMARY_KEY1'
    assert columns[4].get_join() == 's.GDPR = t.GDPR'
    assert columns[6].get_join() is None


@pytest.mark.unit
def test_get_value(columns):
    """Test that value part is escaped and comes from default_value."""
    assert columns[0]._get_value_part() == 'PK1'
    assert columns[2]._get_value_part() == '`ESC`'
    assert columns[3]._get_value_part() == '23'
    assert columns[4]._get_value_part() == 'GDPR'


@pytest.mark.unit
def test_embulk_column_option(columns):
    """Test snowflake to embulk mapping works well."""
    assert columns[0].get_embulk_column_option(
    ) == 'PRIMARY_KEY1: {value_type: string}'
    assert columns[2].get_embulk_column_option(
    ) == 'ESCAPED: {value_type: long}'
    with pytest.raises(TypeError):
        assert columns[-1].get_embulk_column_option()


@pytest.mark.unit
def test_get_clean_data_type(columns):
    """Test if getting clean data type works."""
    assert columns[0]._get_clean_data_type() == 'string'
    assert columns[2]._get_clean_data_type() == 'number'
