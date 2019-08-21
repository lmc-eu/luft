# -*- coding: utf-8 -*-
"""Test Luft Cli."""
from cli.luft import luft

from click.testing import CliRunner

import pytest


@pytest.fixture(scope='session')
def runner():
    """Click runner."""
    return CliRunner()


@pytest.mark.unit
def test_jdbc_load(runner, postgres_db):
    """Test JDBC load."""
    result = runner.invoke(luft, ['jdbc', 'load', '-y', 'world'])
    assert result.exit_code == 0


@pytest.mark.unit
def test_hist_load(runner):
    """Test history load."""
    result = runner.invoke(luft, ['bq', 'load', '-y', 'world'])
    assert result.exit_code == 0
