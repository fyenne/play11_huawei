# %%
import pandas as pd 
import numpy as np 
import os
import re
import warnings

from pandas.io.parsers import ParserBase
warnings.filterwarnings("ignore")
from datetime import date, datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate() 
import sys

# %%
def local_():
    from fyenn_class import  pd_loaddata, pd, np 
    path = './data_down/'
    os.listdir(path)
    huawei_output = pd_loaddata.pd_sep001(path + 'huawei_output.csv')
    rel = pd_loaddata.pd_excel(path + '入湖数据关系', 2)
    rel = rel.drop(['部门', '站点'],axis = 1).drop_duplicates();rel
    pass



def run_etl(start_date, end_date ,env):
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
        ,`_id`
        ,appid
        ,entryid
    FROM ods_public.huawei_output
    WHERE inc_day > '""" + start_date + "'" 
    
    print(sql)
    huawei_output = spark.sql(sql).select("*").toPandas()

    sql2 = """
    select 
    入湖数据关系
    
    """
    print("==================================read_table================================")
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
        ).drop(['createtime', 'updatetime'], axis = 1)
    """
    drop useless cols./
    """
    huawei_output = huawei_output.drop(
        ['id','appid','entryid'], axis = 1
        ).drop_duplicates().sort_values('update_date')
    
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
    relist = ['hon', 'origi', 'pearl', 'guiy', 't_', 'r4_', 'nanh', 'ansh']
    oulist = list(rel['OU（成本中心）'].unique())
    # del dict 
    dict = dict(zip(relist, oulist))
    print(dict)


    # %%
    def concat_(re, ou):
        """
        分组concat all data.
        """
        huawei_output[search_col(huawei_output, re)].shape
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

        # ou 还有 站点名称 带入.
        data['ou'] = ou
        data['station'] = pd.Series(huawei_output.columns).str.extract("(" + re + "[a-z]+)").dropna().iloc[0,0]
        data['addition_type'] = pd.Series(tran_col).str.extract("(" + re + ".+)")[0][0]

        return data.reset_index(drop=True)

    df = pd.DataFrame()
    for re in dict:
        print(re, dict[re])
        df = pd.concat([df, concat_(re, dict[re])], axis = 0)
    df = df.fillna(0)
    print("===============================data_prepared================================")
    print(df.columns)

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
    print("==============================spark_df, env=%s!================================="%env)
    print(spark_df)

    """
    merge table preparation:
    """
   

    merge_table = " table name "
    if env == 'dev':
        merge_table = 'tmp_' + merge_table
        pass
    
    inc_df = spark.sql("""select * from df""")
    print("===============================merge_table--%s================================="%merge_table)
    print(merge_table)
    print('{note:=>50}'.format(note=merge_table) + '{note:=>50}'.format(note=''))
    
    spark.sql("""set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict""")
    # (table_name, df, pk_cols, order_cols, partition_cols=None):
    merge_data = MergeDFToTable(merge_table, inc_df, \
        "worker_name, inc_day", "inc_day", partition_cols="inc_day")
    merge_data.merge()



def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now()).strftime("%Y%m%d")], nargs="*")
    args.add_argument("--end_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now()).strftime("%Y%m%d")], nargs="*")
    args.add_argument("--env", help="dev environment or prod environment", default="dev", nargs="*")

    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    end_date = args_parse.end_date[0]
    env = args_parse.env[0]
 
    run_etl(start_date, end_date, env)

    
if __name__ == '__main__':
    main()

    