"""
Package for running direct sql on files
"""
import pathlib
import argparse
import os
from pyspark.sql import SparkSession

_TRUE_STR: str = "true"
_DATA_FOLDER: str = os.environ.get("SPARK_DATA", "/opt/spark-data")
_APPS_FOLDER: str = os.environ.get("SPARK_APPS", "/opt/spark-apps")

_DF_PARTITIONS_COUNT: int = 20
_SPARK_SESSION: SparkSession
_SEPARATOR: str = ";"


def fn_init_argparse() -> argparse.ArgumentParser:
    """
    Argument parser
    """
    parser = argparse.ArgumentParser(
        usage="%(prog)s [Group ID]",
        description="Executing user defined SQLs groups"
    )
    parser.add_argument(
        "-g", "--group_id", default=None, type=int
    )
    return parser


def fn_create_df_from_csv_file(in_file_name: str,
                               in_separator: str = _SEPARATOR,
                               in_view_name: str = None,
                               in_repartition: int = _DF_PARTITIONS_COUNT):
    """
    Function for registering file in SQL engine as temp view
    """

    l_view_name = in_view_name if in_view_name else in_file_name

    l_df = _SPARK_SESSION.read \
        .option("header", _TRUE_STR) \
        .option("inferSchema", _TRUE_STR) \
        .option("sep", in_separator) \
        .csv(f"file://{_DATA_FOLDER}/{in_file_name}.csv") \
        .repartition(in_repartition) \
        .cache()

    l_df.createTempView(l_view_name)

    return l_df


def fn_init_tables(*args):
    """
    Function for tables initialisation, register existing csv files as SQL views
    """

    for l_one_file in args:
        fn_create_df_from_csv_file(in_file_name=l_one_file)


def fn_run_task(in_tgt_folder: str, in_sql: str,
                in_task_group_id: int = 1, in_repartition_tgt: int = 1):
    """
    Function to run 1 SQL
    """
    l_df = _SPARK_SESSION.sql(in_sql) \
        .repartition(in_repartition_tgt)

    l_df.explain(extended=False)

    l_df.write.mode('overwrite') \
        .parquet(f"{_DATA_FOLDER}/task{in_task_group_id}/{in_tgt_folder}")


def fn_run_task_group(in_task_group_id: int):
    """
    Function to run SQLs in task folder
    """
    global _SPARK_SESSION
    _SPARK_SESSION = SparkSession.builder.appName("py_spark_tasks").getOrCreate()

    fn_init_tables('accounts',
                   'transactions',
                   'country_abbreviation')

    if in_task_group_id is None:
        l_range = range(1, 5)
    else:
        l_range = range(in_task_group_id, in_task_group_id + 1)

    for l_one_task_group_id in l_range:
        l_task_sql_folder = f"{_APPS_FOLDER}/sql/task{l_one_task_group_id}"

        # constructing the path object
        l_dir = pathlib.Path(l_task_sql_folder)

        # iterating the directory
        for l_items in l_dir.iterdir():
            # checking if it's a file
            if l_items.is_file():
                l_sql_file_name = l_items.name.replace(".sql", "")

                with l_items.open(mode="r") as l_sql_file:
                    l_sql_file_content = l_sql_file.read()

                    fn_run_task(in_tgt_folder=l_sql_file_name,
                                in_sql=l_sql_file_content,
                                in_task_group_id=l_one_task_group_id)

    _SPARK_SESSION.stop()


if __name__ == "__main__":
    l_args = fn_init_argparse()
    l_args = l_args.parse_args()
    l_group_id = l_args.group_id

    fn_run_task_group(in_task_group_id=l_group_id)
