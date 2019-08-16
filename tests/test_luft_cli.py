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
