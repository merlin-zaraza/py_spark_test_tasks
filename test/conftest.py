import pytest


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName("pytest-spark").getOrCreate()
