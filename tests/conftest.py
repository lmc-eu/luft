# -*- coding: utf-8 -*-
"""Test Embulk JDBC task."""
import docker

import pytest


@pytest.fixture(scope='function')
def postgres_db():
    """Create Postgres docker container with test data."""
    client = docker.from_env()
    docker_image = 'aa8y/postgres-dataset'
    tag = 'world'
    try:
        client.images.get(docker_image)
    except docker.errors.ImageNotFound:
        client.images.pull(docker_image, tag=tag)
    container = client.containers.create(
        image=docker_image + ':' + tag,
        name='postgres',
        detach=True,
        ports={'5432/tcp': 5432}
    )
    container.start()
    yield container
    container.remove(force=True)
