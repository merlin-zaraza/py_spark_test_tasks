"""
Package for running sql on Dataframes
"""
import pathlib
from pyspark.sql import SparkSession

_TRUE_STR: str = "true"
_data_folder: str = "/opt/spark-data"
_apps_folder: str = "/opt/spark-apps"
_df_partitions_count: int = 20
_spark_session: SparkSession
_task_num: int = 1
_separator: str = ";"


def fn_create_df_from_csv_file(in_file_name: str,
                               in_separator: str = _separator,
                               in_view_name: str = None,
                               in_repartition: int = _df_partitions_count):
    """
    Function for registering file in SQL engine as temp view
    """

    l_view_name = in_view_name if in_view_name else in_file_name

    l_df = _spark_session.read \
        .option("header", _TRUE_STR) \
        .option("inferSchema", _TRUE_STR) \
        .option("sep", in_separator) \
        .csv(f"file://{_data_folder}/{in_file_name}.csv") \
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


def fn_run_task(in_tgt_folder: str
                , in_sql: str
                , in_task_group_id: int = _task_num
                , in_repartition_tgt: int = 1):
    """
    Function to run 1 SQL
    """
    l_df = _spark_session.sql(in_sql) \
        .repartition(in_repartition_tgt)

    l_df.explain(extended=False)

    l_df.write.mode('overwrite') \
        .parquet(f"{_data_folder}/task_{in_task_group_id}/{in_tgt_folder}")


def fn_run_task_group(in_task_group_id: int):
    """
    Function to run SQLs in task folder
    """
    l_task_sql_folder = f"{_apps_folder}/sql/task{in_task_group_id}"

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
                            in_task_group_id=in_task_group_id)


if __name__ == "__main__":
    _spark_session = SparkSession.builder.appName("py_spark_tasks").getOrCreate()

    fn_init_tables('accounts',
                   'transactions',
                   'country_abbreviation')

    for l_one_task_group in range(1, 3):
        fn_run_task_group(in_task_group_id=l_one_task_group)

    _spark_session.stop()
