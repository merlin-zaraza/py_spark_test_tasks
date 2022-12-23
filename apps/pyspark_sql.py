"""
Package for
"""
from typing import Dict

from pyspark.sql import SparkSession

_TRUE_STR: str = "true"
_data_folder: str = "/opt/spark-data"
_df_partitions_count: int = 20
_spark_session: SparkSession = SparkSession.builder.appName("py_spark_tasks").getOrCreate()

"""
Function for registering file in SQL engine
"""


def fn_create_df_from_csv_file(in_file_name: str,
                               in_header: str = _TRUE_STR,
                               in_infer_schema: str = _TRUE_STR,
                               in_separator: str = ";",
                               in_view_name: str = None,
                               in_repartition: int = _df_partitions_count):
    l_view_name = in_view_name if in_view_name else in_file_name

    l_df = _spark_session.read \
        .option("header", in_header) \
        .option("inferSchema", in_infer_schema) \
        .option("sep", in_separator) \
        .csv(f"file://{_data_folder}/{in_file_name}.csv") \
        .repartition(in_repartition) \
        .cache()

    l_df.createTempView(l_view_name)

    return l_df


def fn_init_tables(in_src_tables: Dict):
    for l_one_file, l_repartition in in_src_tables.items():
        fn_create_df_from_csv_file(in_file_name=l_one_file, in_repartition=l_repartition)


def fn_run_task(in_tgt_table: str
                , in_sql: str
                , in_task_id: int
                , in_repartition_tgt: int = _df_partitions_count):
    _spark_session.sql(in_sql).repartition(in_repartition_tgt) \
        .write.mode('overwrite').csv(f"{_data_folder}/task_{in_task_id}/{in_tgt_table}_sql.csv")
    # .write.mode('overwrite').parquet(f"{_data_folder}/{in_tgt_table}")


fn_init_tables({'accounts': _df_partitions_count,  # id, first_name, last_name, age, country
                'transactions': _df_partitions_count,  # id, amount, account_type, transaction_date, country
                'country_abbreviation': 4  # country_full_name, abbreviation
                })

fn_run_task(in_tgt_table="account_types",
            in_sql="Select account_type, count(*) as cnt "
                   "From transactions "
                   "Group by account_type",
            in_task_id=1,
            in_repartition_tgt=1)

fn_run_task(in_tgt_table="account_balance",
            in_sql="Select id, sum(amount) as balance, max(transaction_date) as latest_date "
                   "From transactions "
                   "Group by id",
            in_task_id=1)

fn_run_task(in_tgt_table="accounts_btw_18_30",
            in_sql="Select id, first_name, last_name, age, country "
                   "From accounts "
                   "Where age between 18 and 30",
            in_task_id=2)

fn_run_task(in_tgt_table="accounts_non_pro",
            in_sql="Select id, count(*) as cnt "
                   "From transactions "
                   "Where account_type !='Professional' group by id",
            in_task_id=2)

fn_run_task(in_tgt_table="accounts_top_5",
            in_sql="Select first_name, count(*) as cnt from accounts "
                   "Group by first_name "
                   "Order by cnt desc limit 5",
            in_task_id=2)

fn_run_task(in_tgt_table="total_expenses",
            in_sql="Select id, "
                   "  sum( case when amount < 0 then amount else 0 end ) as expenses, "
                   "  sum( case when amount > 0 then amount else 0 end ) as earnings "
                   "From transactions "
                   "Group by id",
            in_task_id=2)

fn_run_task(in_tgt_table="total_expenses_pivot",
            in_sql="Select id, "
                   "  sum( case when amount < 0 then amount else 0 end ) as expenses, "
                   "  sum( case when amount > 0 then amount else 0 end ) as earnings "
                   "From transactions "
                   "Group by id",
            in_task_id=2)

_spark_session.stop()
