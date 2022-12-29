import pytest
import pyspark_dataframe as psd


@pytest.mark.spark
def test_task1_1():
    psd.fn_run_test_task(1, 1)
