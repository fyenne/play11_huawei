# %%
import pandas as pd 
import numpy as np 
import os
import re
import warnings

from pandas.io.parsers import ParserBase
warnings.filterwarnings("ignore")
from datetime import date, datetime, timedelta
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate() 
import sys

# %%
# def local_():
#     from fyenn_class import  pd_loaddata, pd, np 
#     path = './data_down/'
#     os.listdir(path)
#     huawei_output = pd_loaddata.pd_sep001(path + 'huawei_output.csv')
#     rel = pd_loaddata.pd_excel(path + '入湖数据关系', 2)
#     rel = rel.drop(['部门', '站点'],axis = 1).drop_duplicates();rel
#     pass



def run_etl(start_date, env):
    print("python version here:", sys.version, '\t') 
    print("===================================sysVersion================================")
    print("list dir", os.listdir())
    
        
    sql = """
    SELECT creator
        ,updater
        ,deleter
        ,createtime
        ,updatetime
        ,deletetime
        ,update_date
        ,origin_receive
        ,origin_send
        ,origin_psn
        ,honor_receive
        ,honer_send
        ,honor_psn
        ,pearlriver_transport_times
        ,guiyang_transport_times
        ,t_product_receive
        ,t_product_clean
        ,t_product_send
        ,r4_receive
        ,r4_send
        ,r4_psn
        ,nanhua_receive
        ,nanhua_send
        ,nanhau_psn
        ,anshi_receive
        ,anshi_send
        ,anshi_case
        ,te_origin_receive
        ,te_origin_send
        ,te_product_receive
        ,te_product_send
        ,pre_receive
        ,pre_withdraw
        ,honor_transport_times
    FROM ods_public.huawei_output
    WHERE inc_day = '""" + start_date + "'" 
    
    print(sql)
    huawei_output = spark.sql(sql).select("*").toPandas()

    

    print("==================================read_table%s================================"%env)
    print(huawei_output.head())

    def datetime_(coach, col):
        """
        detail time to date; 
        split and assign to 3 cols of year m d
        """
        coach[col] = coach[col].astype(str).str.slice(0,10)
        coach = pd.concat([coach, pd.DataFrame(list(coach[col].str.split('-')))], axis =1)
        coach = coach[coach[[0,1,2]].astype(int).sum(axis = 1) != 0]
        coach = coach.rename({0:'year', 1:'month', 2: 'date'}, axis=1)
        return coach

    huawei_output = datetime_(
        coach=huawei_output, col='update_date'
        ).drop(['createtime', 'updatetime'], axis = 1).drop_duplicates().sort_values('update_date')

    """
    drop useless cols./
    """
    
    def search_col(df, str):
        """
        列名正则搜索.~
        """
        return list(pd.Series(df.columns)[pd.Series(df.columns).str.match(str)])


    # %%
    # 识别异常列名
    tran_col = list(pd.Series(huawei_output.columns)[
        pd.Series(huawei_output.columns).str.contains(
            '(receive|psn|send|transport_times|update_date|year|month|date|id)'
            ) == False
            ])
    # clean , case, withdraw)

    # %%
    # re = 'pear'
    # pd.Series(huawei_output.columns).str.extract("(" + re + "[a-z]+)").dropna()
    """
    ou 和正则匹配
    """
    relist = ['origi', 'hon', 'pearl', 'guiy', '^t\_', 'r4\_', 'nanh', 'ansh', 't4']
    oulist = ['HUAWEDHW4S',
        'HONORDGHMS',
        'HUAWEDHWTS',
        'HUAWEDGTRD',
        'HUAWEDGLSS',
        'HUAWEDHW1S',
        'HUAWEDGNHS',
        'NEXPEDGWHS',
        'TYCOTSDXXS',]
    # del dict 
    my_dict = dict(zip(relist, oulist))
    print(my_dict)
    """
    te 站点的modify, 将 te_origin 和 te_product 合并相加.
    """
    huawei_output['te_origin_receive'] = huawei_output['te_origin_receive'] + huawei_output['te_product_receive'] 
    huawei_output['te_origin_send'] = huawei_output['te_origin_send'] + huawei_output['te_product_send'] 
    huawei_output = huawei_output.drop(['te_product_send', 'te_product_receive'], axis = 1)



    # %%
    def concat_(re, ou):
        # huawei_output[search_col(huawei_output, re)].shape
        # n = 4 - huawei_output[search_col(huawei_output, re)].shape[1]
        # m = huawei_output[search_col(huawei_output, re)].shape[0]
        # print(m, n)
        data = pd.concat(
            [
                huawei_output[search_col(huawei_output, re)], 
                # pd.DataFrame(np.zeros(shape=(m, n), dtype=int)),
                huawei_output[['update_date', 'year', 'month', 'date']]
            ]
            , axis = 1).sort_values('update_date')
        # print(data.shape)
        data.columns = list(pd.Series(data.columns).str.extract(
            '(receive|psn|send|transport_times|update_date|year|month|date)'
            )[0])
        try:
            data = data.rename({np.nan:'addition'}, axis = 1)
        except:
            pass
        data['ou'] = ou
        data['station'] = pd.Series(huawei_output.columns)\
            .str.extract("(" + re + "[a-z]+)").dropna().iloc[0,0]
        data['addition_type'] = pd.Series(tran_col)\
            .str.extract("(" + re + ".+)").fillna('z').sort_values(0).iloc[0,0]
        
        return data.reset_index(drop=True)

    df = pd.DataFrame()
    for re in my_dict:
        print(re, my_dict[re])
        df = pd.concat([df, concat_(re, my_dict[re])], axis = 0)

    
    def cleanm(df):
        """
        清楚每日填报重复, 时间日期drop, update_date 日期加一以匹配前端.
        """
        df = df.fillna(0).drop_duplicates()
        df['flag_sum'] = df[['receive', 'send', 'psn']].sum(axis =1)
        df = df.sort_values(['update_date', 'ou', 'flag_sum'], ascending=False).groupby(
            [
                'update_date', 'ou'
            ]
            ).first().reset_index()
        try:
            df = df.drop(['year', 'month', 'date', 'flag_sum'], axis = 1)
        except:
            pass
        df['update_date'] = pd.to_datetime(df['update_date']) + timedelta(days = 1)
        df['update_date'] = df['update_date'].astype(str)
        df['addition_type'] = df['addition_type'].str.replace('^z', 'None')
        return df
    df = cleanm(df)
    df['inc_day'] = start_date
    print("===============================data_prepared================================")
    df[['receive', 'send', 'psn', 'transport_times', 'addition']] = df[
        ['receive', 'send', 'psn', 'transport_times', 'addition']
        ].astype(int)
    print(df.info())

    # %%
    # df.query("year == '2021' & month == '05' & date == '29'")
    # list(pd.Series(huawei_output.columns).str.extract("(" + 'pearl' + "[a-z]+)").dropna()[0])[0]
    """
    to bdp
    """
    # pd to spark table
    spark_df = spark.createDataFrame(df)
    # spark table as view, aka in to spark env. able to be selected or run by spark sql in the following part.
    spark_df.createOrReplaceTempView("df")
    # 
    print(env)
    print("==============================spark_df, env=%s!================================="%env)
    print(spark_df)

    """
    merge table preparation:
    """
   

    merge_table = "dsc_dws.dws_dsc_huawei_operation_sum_df"
    if env == 'dev':
        merge_table = 'tmp_' + merge_table
    else:
        pass
    print('看一下merge_table from john')
    print(merge_table)
    
    inc_df = spark.sql("""select * from df""")
    print("===============================merge_table--%s================================="%merge_table)
    # merge_table = "tmp_dsc_dws.dws_dsc_huawei_operation_sum_df"
    # print(merge_table)
    print('{note:=>50}'.format(note=merge_table) + '{note:=>50}'.format(note=''))

    spark.sql("""set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict""")
    # (table_name, df, pk_cols, order_cols, partition_cols=None):
    merge_data = MergeDFToTable(merge_table, inc_df, \
        "ou, update_date, inc_day", "inc_day", partition_cols="inc_day")
    merge_data.merge()



def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now()).strftime("%Y%m%d")], nargs="*")

    args.add_argument("--env", help="dev environment or prod environment", default="dev", nargs="*")

    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    env = args_parse.env[0]
 
    run_etl(start_date, env)

    
if __name__ == '__main__':
    main()

    