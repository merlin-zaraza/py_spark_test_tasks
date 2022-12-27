"""
Package for running direct sql on files
"""
from pyspark.sql import functions as f

import test_pyspark_sql as t
from test_pyspark_sql import TaskDf, \
    fn_get_tasks_range, fn_clean_up_data_folder, fn_run_tasks_by_definition_list, \
    fn_create_df_from_csv_file

l_accounts_df = fn_create_df_from_csv_file(in_file_name=t.ACCOUNTS)
l_transactions_df = fn_create_df_from_csv_file(in_file_name=t.TRANSACTIONS)
l_country_abbreviation_df = fn_create_df_from_csv_file(in_file_name=t.COUNTRY_ABBREVIATION)


def fn_get_task1_def_list():
    """
    Task 1 Data Frames List
    """

    l_df_account_types_count = l_transactions_df \
        .groupBy(f.col("account_type")) \
        .agg(f.count("account_type").alias("cnt"))

    l_df_account_balance = l_transactions_df \
        .groupBy(f.col("id")) \
        .agg(f.sum("amount").alias("balance"), f.max("transaction_date").alias("latest_date"))

    return [
        TaskDf("1.1_account_types_count", l_df_account_types_count),
        TaskDf("1.2_account_balance", l_df_account_balance),
    ]


def fn_get_task2_def_list():
    """
    Task 2 Data Frames List
    """

    l_df_accounts_btw_18_30 = l_accounts_df.selectExpr(
        "id",
        "first_name",
        "last_name",
        "age",
        "country"
    ).where("age between 18 and 30")

    l_df_accounts_non_pro = l_transactions_df.selectExpr(
        "id",
        "account_type"
    ).where(f.col("account_type") != 'Professional') \
        .groupBy("id") \
        .agg(f.count("id").alias("cnt"))

    l_df_accounts_top5 = l_accounts_df \
        .groupBy("first_name") \
        .agg(f.count("first_name").alias("cnt")) \
        .orderBy(f.col("cnt").desc()) \
        .limit(5)

    l_df_total_expenses = l_transactions_df.selectExpr(
        "id",
        " case when amount < 0 then amount else 0 end as expenses",
        " case when amount > 0 then amount else 0 end as earnings "
    ).groupBy("id") \
        .agg(f.sum("expenses").alias("expenses"),
             f.sum("earnings").alias("earnings"))

    l_df_total_expenses_pivot = l_transactions_df.selectExpr(
        "id",
        "amount",
        " case when amount < 0 then 'expenses' else 'earnings' end as expenses_type",
    ).groupBy("id") \
        .pivot("expenses_type") \
        .agg(f.sum("amount").alias("amount"))

    return [
        TaskDf("2.1_accounts_btw_18_30", l_df_accounts_btw_18_30),
        TaskDf("2.2_accounts_non_pro", l_df_accounts_non_pro),
        TaskDf("2.3_accounts_top_5", l_df_accounts_top5),
        TaskDf("2.4_total_expenses", l_df_total_expenses),
        TaskDf("2.5_total_expenses_pivot", l_df_total_expenses_pivot),
    ]


def fn_get_task3_def_list():
    """
    Task 3 Data Frames List
    """

    l_df_first_last_concatenated = l_accounts_df.selectExpr(
        "Concat(first_name,last_name) as first_last_concat"
    ).where("age between 18 and 30")

    l_df_avg_transaction_amount_2021_per_client = l_transactions_df.selectExpr(
        "id",
        "amount"
    ).where("transaction_date like '2021%'") \
        .groupBy("id") \
        .agg(f.round(f.avg("amount"),
                     2).alias("avg_amount"))

    l_df_account_types_count = l_transactions_df \
        .groupBy(f.col("account_type")) \
        .agg(f.count("account_type").alias("cnt"))

    l_df_top_10_positive = l_transactions_df.where("amount > 0") \
        .groupBy("id") \
        .agg(f.round(f.sum("amount"),
                     2).alias("total_amount")) \
        .orderBy(f.col("total_amount").desc()) \
        .limit(10)

    l_df_clients_sorted_by_first_name_descending = l_accounts_df \
        .select("first_name") \
        .orderBy(f.col("first_name").desc())

    return [
        TaskDf("3.1_first_last_concatenated", l_df_first_last_concatenated),
        TaskDf("3.2_avg_transaction_amount_2021_per_client", l_df_avg_transaction_amount_2021_per_client),
        TaskDf("3.3_account_types_count", l_df_account_types_count),
        TaskDf("3.4_top_10_positive", l_df_top_10_positive),
        TaskDf("3.5_clients_sorted_by_first_name_descending", l_df_clients_sorted_by_first_name_descending),
    ]


def fn_run_dataframe_task(in_task_group_id: int):
    """
    Function to execute all DF from task group list
    and put them to the /opt/spark-data/df folder
    """

    l_range = fn_get_tasks_range(in_task_group_id)

    fn_clean_up_data_folder(in_task_group_id=in_task_group_id,
                            in_task_type=t.TAKS_TYPE_DF)

    for l_one_task_group in l_range:

        if l_one_task_group == 1:
            l_one_task_definition_list = fn_get_task1_def_list()
        elif l_one_task_group == 2:
            l_one_task_definition_list = fn_get_task2_def_list()
        elif l_one_task_group == 3:
            l_one_task_definition_list = fn_get_task3_def_list()
        else:
            raise ValueError(f"Invalid Value for l_one_task_group : {l_one_task_group}")

        fn_run_tasks_by_definition_list(in_task_group_id=l_range,
                                        in_task_definition_list=l_one_task_definition_list)


if __name__ == "__main__":
    l_args = t.fn_init_argparse()
    l_args = l_args.parse_args()
    l_group_id = l_args.group_id

    fn_run_dataframe_task(in_task_group_id=l_group_id)
