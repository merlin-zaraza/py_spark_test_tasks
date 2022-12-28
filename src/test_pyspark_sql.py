"""
Package for running direct sql on files
"""
import pathlib
import argparse
import os
import shutil
from collections import namedtuple
from typing import List

from pyspark.sql import SparkSession, DataFrame

ACCOUNTS = 'accounts'
TRANSACTIONS = 'transactions'
COUNTRY_ABBREVIATION = 'country_abbreviation'

TAKS_TYPE_SQL = "sql"
TAKS_TYPE_DF = "df"

_APP_NAME = "py_spark_tasks"

_TRUE_STR: str = "true"
_DATA_FOLDER: str = os.environ.get("SPARK_DATA", "/opt/spark-data")
_APPS_FOLDER: str = os.environ.get("SPARK_APPS", "/opt/spark-apps")

_DF_PARTITIONS_COUNT: int = 20
_SPARK_SESSION: SparkSession = SparkSession.builder.appName(_APP_NAME).getOrCreate()
_SEPARATOR: str = ";"
TaskDf = namedtuple("TaskDf", "tgt_folder data_frame")


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


def fn_get_task_target_folder(in_task_type: str,
                              in_task_group_id: int = None,
                              in_tgt_folder: str = ""):
    """
    Gives path to target folder (for task or group)
    """

    l_task_group_type_folder = f"{_DATA_FOLDER}/{in_task_type}"
    l_one_task_folder = f"{l_task_group_type_folder}/task{in_task_group_id}/{in_tgt_folder}"

    if in_task_group_id is None:
        return l_task_group_type_folder

    return l_one_task_folder


def fn_run_task(in_tgt_folder: str,
                in_data_frame: DataFrame = None,
                in_sql: str = None,
                in_task_group_id: int = 1,
                in_repartition_tgt: int = None,
                in_mode: str = "overwrite"):
    """
    Function to write Data Frame Output to file using DF or Spark SQL
    """

    l_type = TAKS_TYPE_DF
    l_df = in_data_frame

    if in_sql:
        l_type = TAKS_TYPE_SQL
        l_df = _SPARK_SESSION.sql(in_sql)
        l_df.explain(extended=False)

    l_path = fn_get_task_target_folder(in_task_group_id=in_task_group_id,
                                       in_task_type=l_type,
                                       in_tgt_folder=in_tgt_folder)

    if (in_repartition_tgt is not None) and in_repartition_tgt > 0:
        l_df.repartition(in_repartition_tgt) \
            .write.mode(in_mode) \
            .option("header", _TRUE_STR) \
            .option("inferSchema", _TRUE_STR) \
            .option("sep", _SEPARATOR) \
            .csv(l_path)
    else:
        l_df.write.mode(in_mode) \
            .option("header", _TRUE_STR) \
            .option("inferSchema", _TRUE_STR) \
            .option("sep", _SEPARATOR) \
            .csv(l_path)

    l_df.show()


def fn_run_tasks_by_definition_list(in_task_group_id: int,
                                    in_task_definition_list: List[TaskDf]):
    """
    Function to execute DF functions based on DF list
    """

    for l_one_task_df in in_task_definition_list:
        fn_run_task_df(in_task_group_id=in_task_group_id,
                       in_tgt_folder=l_one_task_df.tgt_folder,
                       in_data_frame=l_one_task_df.data_frame,
                       in_repartition_tgt=1)


def fn_run_task_sql(in_tgt_folder: str,
                    in_sql: str,
                    in_task_group_id: int = 1,
                    in_repartition_tgt: int = None):
    """
    Function to run 1 SQL
    """

    fn_run_task(in_tgt_folder=in_tgt_folder,
                in_sql=in_sql,
                in_task_group_id=in_task_group_id,
                in_repartition_tgt=in_repartition_tgt)


def fn_run_task_df(in_tgt_folder: str,
                   in_data_frame: DataFrame,
                   in_task_group_id: int = 1,
                   in_repartition_tgt: int = None):
    """
    Function to explain and run one DF
    """
    fn_run_task(in_tgt_folder=in_tgt_folder,
                in_data_frame=in_data_frame,
                in_task_group_id=in_task_group_id,
                in_repartition_tgt=in_repartition_tgt)


def fn_get_tasks_range(in_task_group_id: int):
    """
    Function to get list of tasks to run
    By default (in_task_group_id is none) it is all tasks : 1,2,3,4
    Otherwise only one task
    """
    l_default_range = range(1, 5)

    if in_task_group_id is None:
        l_range = l_default_range
    else:
        if in_task_group_id not in l_default_range:
            raise ValueError(f"in_task_group_id is out of range : {in_task_group_id} not in {l_default_range}")

        l_range = range(in_task_group_id, in_task_group_id + 1)

    return l_range


def fn_init(in_init_all_tables: bool = False):
    """
    Function to init spark tables and spark session
    """

    global _SPARK_SESSION
    _SPARK_SESSION = SparkSession.builder.appName(_APP_NAME).getOrCreate()

    if in_init_all_tables:
        fn_init_tables(ACCOUNTS,
                       TRANSACTIONS,
                       COUNTRY_ABBREVIATION)


def fn_close_session():
    """
    Function to close spark session
    """
    _SPARK_SESSION.stop()


def fn_clean_up_data_folder(in_task_group_id: int,
                            in_task_type: str):
    """
    Function to clean up data folder before task execution
    """

    l_group_path = fn_get_task_target_folder(in_task_group_id=in_task_group_id,
                                             in_task_type=in_task_type)

    shutil.rmtree(l_group_path, onerror=FileNotFoundError)


def fn_run_task_group_sql(in_task_group_id: int):
    """
    Function to run SQLs in task folder
    """

    fn_init(in_init_all_tables=True)

    l_range = fn_get_tasks_range(in_task_group_id)

    fn_clean_up_data_folder(in_task_group_id=in_task_group_id,
                            in_task_type=TAKS_TYPE_SQL)

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

                    fn_run_task_sql(in_tgt_folder=l_sql_file_name,
                                    in_sql=l_sql_file_content,
                                    in_task_group_id=l_one_task_group_id,
                                    in_repartition_tgt=1)

    fn_close_session()


if __name__ == "__main__":
    l_args = fn_init_argparse()
    l_args = l_args.parse_args()
    l_group_id = l_args.group_id

    fn_run_task_group_sql(in_task_group_id=l_group_id)
