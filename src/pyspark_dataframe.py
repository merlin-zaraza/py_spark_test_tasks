"""
Package for running direct sql on files
"""
import sys
from typing import Dict, List
from pyspark.sql import functions as f, DataFrame

import pyspark_sql as t
from pyspark_sql import TaskDf


def fn_get_task_def_list1(in_group_id) -> List[TaskDf]:
    """
    Task 1 Data Frames List
    """

    l_df_account_types_count = DF_TRANSACTIONS \
        .groupBy("account_type") \
        .agg(f.countDistinct("id").alias("cnt"))

    l_df_account_balance = DF_TRANSACTIONS \
        .groupBy(f.col("id")) \
        .agg(f.round(f.sum("amount"), t.ROUND_DIGITS).alias("balance"),
             f.max("transaction_date").alias("latest_date"))

    return [
        TaskDf(in_group_id, "1.1_account_types_count", l_df_account_types_count),
        TaskDf(in_group_id, "1.2_account_balance", l_df_account_balance, "id <= 20 "),
    ]


def fn_inner_join_acc_names_to_df(in_dataframe: t.DataFrame) -> t.DataFrame:
    """
    Inner join of "first_name", "last_name" by id
    """

    return in_dataframe.join(
        f.broadcast(DF_ACCOUNTS.select("id", "first_name", "last_name")),
        "id",
        "inner")


def fn_get_task_def_list2(in_group_id) -> List[TaskDf]:
    """
    Task 2 Data Frames List
    """

    l_df_accounts_btw_18_30 = DF_ACCOUNTS.selectExpr(
        "id",
        "first_name",
        "last_name",
        "age",
        "country"
    ).where("age between 18 and 30")

    l_df_accounts_non_pro = DF_TRANSACTIONS.selectExpr(
        "id",
        "account_type"
    ).where(f.col("account_type") != 'Professional') \
        .groupBy("id") \
        .agg(f.count("id").alias("cnt"))

    l_l_df_accounts_non_pro_with_user_info = fn_inner_join_acc_names_to_df(l_df_accounts_non_pro)

    l_df_accounts_top5 = DF_ACCOUNTS \
        .groupBy("first_name") \
        .agg(f.count("first_name").alias("cnt")) \
        .orderBy(f.col("cnt").desc()) \
        .limit(5)

    l_df_total_expenses = DF_TRANSACTIONS.selectExpr(
        "id",
        " case when amount < 0 then amount else 0 end as expenses",
        " case when amount > 0 then amount else 0 end as earnings "
    ).groupBy("id") \
        .agg(f.round(f.abs(f.sum("expenses")), t.ROUND_DIGITS).alias("expenses"),
             f.round(f.sum("earnings"), t.ROUND_DIGITS).alias("earnings"))

    l_df_total_expenses_with_user_info = fn_inner_join_acc_names_to_df(l_df_total_expenses)

    l_df_total_expenses_pivot = DF_TRANSACTIONS.selectExpr(
        "id",
        "case when amount > 0 then amount else 0 end as earnings",
        "int(substring(transaction_date, 1, 4)) as tr_year",
    ).groupBy("id") \
        .pivot("tr_year") \
        .agg(f.round(f.sum("earnings").alias("earnings"), t.ROUND_DIGITS)) \
        .fillna(value=0)

    return [
        TaskDf(in_group_id, "2.1_accounts_btw_18_30", l_df_accounts_btw_18_30,
               "id in (1,5,6,8,19,30,33,34,35,36,38,42,44,52,55,57,64,72,74,76)"),
        TaskDf(in_group_id, "2.2_accounts_non_pro", l_l_df_accounts_non_pro_with_user_info, "id <= 20"),
        TaskDf(in_group_id, "2.3_accounts_top_5", l_df_accounts_top5),
        TaskDf(in_group_id, "2.4_total_per_year", l_df_total_expenses_with_user_info,
               "id in (351901,64444,42093,456473,372636,457272,170685,153318,288955,452806,"
               "435985,248093,111744,392651,180469,204816,263364,230316,56785,109722)"),
        TaskDf(in_group_id, "2.5_total_earnings_pivot", l_df_total_expenses_pivot, "id <= 20"),
    ]


def fn_get_task_def_list3(in_group_id) -> List[TaskDf]:
    """
    Task 3 Data Frames List
    """

    l_df_first_last_concatenated = DF_ACCOUNTS \
        .selectExpr("concat(first_name,  ' ', last_name) as first_last_concat") \
        .where("age between 18 and 30").distinct()

    l_df_avg_transaction_amount_2021_per_client = DF_TRANSACTIONS.selectExpr(
        "id",
        "amount"
    ).where("transaction_date like '2021%'") \
        .groupBy("id") \
        .agg(f.round(f.avg("amount"), t.ROUND_DIGITS).alias("avg_amount"))

    l_df_account_types_count = DF_TRANSACTIONS \
        .groupBy(f.col("account_type")) \
        .agg(f.countDistinct("id").alias("cnt"))

    l_df_top_10_positive = DF_TRANSACTIONS.where("amount > 0") \
        .groupBy("id") \
        .agg(f.round(f.sum("amount"), t.ROUND_DIGITS).alias("total_amount")) \
        .orderBy(f.col("total_amount").desc()) \
        .limit(10)

    l_df_clients_sorted_by_first_name_descending = DF_ACCOUNTS \
        .select("first_name", "last_name").distinct() \
        .orderBy(f.col("first_name").desc())

    return [
        TaskDf(in_group_id, "3.1_first_last_concatenated", l_df_first_last_concatenated, """
           first_last_concat in ('Darcy Phillips','Amelia Wright','Haris Ellis',
           'Tony Hall','Rubie Stewart','Miley Perry','Marcus Carter','Charlie Harris','Honey Rogers','Luke Harris',
           'Spike Murphy','Vincent Adams','James Barnes','George Bailey','Sienna Holmes','Isabella Elliott',
           'Freddie Martin','Kate Wright','Albert Myers','Connie Wells')
         """),
        TaskDf(in_group_id, "3.2_avg_transaction_amount_2021_per_client",
               l_df_avg_transaction_amount_2021_per_client,
               "id in ( 1,2,4,6,7,11,12,13,15,17,19,22,23,24,27,28,30,31,32,33 )"),
        TaskDf(in_group_id, "3.3_account_types_count", l_df_account_types_count),
        TaskDf(in_group_id, "3.4_top_10_positive", l_df_top_10_positive),
        TaskDf(in_group_id, "3.5_clients_sorted_by_first_name_descending",
               l_df_clients_sorted_by_first_name_descending,
               "first_name in ('Wilson') and "
               "last_name  in ('Mitchell','Anderson','Cameron','Gray','Barnes',"
               "'Williams','Stewart','Elliott','Cole',"
               "'Tucker','Stewart','Ferguson','Davis','Higgins','Perry','Riley',"
               "'Edwards','Richards','Myers','Johnson')"
               ),
    ]


def fn_get_richest_person_in_country_broadcast():
    """
    DF for richest person using broadcast
    """

    l_richest_person_transactions = DF_TRANSACTIONS.selectExpr(
        "id",
        "amount"
    ).groupBy("id") \
        .agg(
        f.round(f.sum("amount"), t.ROUND_DIGITS).alias("total_amount")
    )

    l_df_richest_person_account_info = l_richest_person_transactions.join(
        f.broadcast(DF_ACCOUNTS),
        "id",
        "inner"
    ).withColumn(colName="rn", col=f.expr("row_number() over (partition by country order by total_amount desc)")) \
        .selectExpr(
        "id",
        "first_name",
        "last_name",
        "country",
        "total_amount",
    ).where("rn  == 1 ")

    l_df_richest_person_all_info = l_df_richest_person_account_info.join(
        f.broadcast(DF_COUNTRY_ABBR),
        DF_COUNTRY_ABBR.abbreviation == l_df_richest_person_account_info.country,
        "inner"
    ).drop("abbreviation", "country", "id")

    return l_df_richest_person_all_info


def fn_get_invalid_accounts():
    """
    DF for invalid accounts using broadcast
    """

    l_df_tr_filtered = DF_TRANSACTIONS \
        .where(" account_type = 'Professional' ") \
        .drop("country")

    l_df_acc_filtered = DF_ACCOUNTS.where(" age < 26 ") \
        .withColumnRenamed("id", "account_id")

    l_df_tr_invalid_acc = l_df_tr_filtered.join(f.broadcast(l_df_acc_filtered),
                                                l_df_acc_filtered.account_id == l_df_tr_filtered.id,
                                                "inner")

    return l_df_tr_invalid_acc


def fn_get_all_info_broadcast():
    """
    DF for all data in one place using broadcast
    """

    l_df_trans_info = DF_TRANSACTIONS \
        .select("id",
                "amount",
                "account_type") \
        .groupBy("id", "account_type") \
        .agg(f.round(f.sum("amount"), t.ROUND_DIGITS).alias("total_amount"))
    # f.concat_ws(",", f.collect_list("account_type")).alias("account_types")

    l_df_trans_and_acc_info = l_df_trans_info \
        .join(f.broadcast(DF_ACCOUNTS),
              "id",
              "inner") \
        .withColumnRenamed("country", "abbreviation")

    l_df_all_info = l_df_trans_and_acc_info \
        .join(f.broadcast(DF_COUNTRY_ABBR),
              "abbreviation",
              "inner").drop("abbreviation")

    return l_df_all_info


def fn_get_task_def_list4(in_group_id) -> List[TaskDf]:
    """
    Task 4 Data Frames List
    """
    return [
        TaskDf(in_group_id, "4.1_person_with_biggest_balance_in_country",
               fn_get_richest_person_in_country_broadcast(),
               "country_full_name in ('Bulgaria','Surinam','Mauritius','Chile','Ethiopia','Peru','Mali',"
               "'Malawi','Senegal','Spain','Cuba','Belgium','Yemen','Denmark','Belgium','Ecuador',"
               "'Honduras','Peru','El Salvador','China')"),
        TaskDf(in_group_id, "4.2_invalid_accounts", fn_get_invalid_accounts(),
               "account_type = 'Professional' and account_id in (7253) "),
        TaskDf(in_group_id, "4.3_single_dataset", fn_get_all_info_broadcast(),
               "id in (1,6,12,13,16,22,26)")
    ]


def fn_get_dict_with_all_tasks() -> Dict[int, List[TaskDf]]:
    """
    Returns Dictionary with all task groups inside
    """
    l_result = {}

    for l_one_task_id in t.fn_get_task_group_range():
        fn_get_task_def_list = getattr(sys.modules[__name__], f'fn_get_task_def_list{l_one_task_id}')
        l_result.setdefault(l_one_task_id, fn_get_task_def_list(l_one_task_id))

    return l_result


l_all_df_dict = t.fn_init_tables()

DF_ACCOUNTS: DataFrame = l_all_df_dict[t.ACCOUNTS]
DF_TRANSACTIONS: DataFrame = l_all_df_dict[t.TRANSACTIONS]
DF_COUNTRY_ABBR: DataFrame = l_all_df_dict[t.COUNTRY_ABBREVIATION]

DICT_ALL_GROUP_TASKS = fn_get_dict_with_all_tasks()

if __name__ == "__main__":
    l_args = t.fn_init_argparse(t.TASK_TYPE_DF)
    l_args = l_args.parse_args()
    l_group_id = l_args.group_id
    l_task_id = l_args.task_id
    l_task_type = l_args.task_type

    t.fn_run_task_type(in_task_group_id=l_group_id,
                       in_task_id=l_task_id,
                       in_task_type=l_task_type,
                       in_dict_all_group_tasks=DICT_ALL_GROUP_TASKS)
