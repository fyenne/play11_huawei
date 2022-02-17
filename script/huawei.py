# %%
import pandas as pd 
import numpy as np 
import os
import re
import warnings
from MergeDataFrameToTable import * 
from pandas.io.parsers import ParserBase
warnings.filterwarnings("ignore")
from datetime import date, datetime, timedelta
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *  
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



def run_etl(start_date, env, regexp, ou_code, work_hour_date_range):
    
    print("python version here:", sys.version, '\t') 
    print("===================================sysVersion================================")
    # def printer(*args):
    #     [print( '{note:~>25}'.format(note = i)) for i in args]
    regexp = regexp[0].split(',')
    ou_code = ou_code[0].split(',')
    print("my parameters",  print(start_date, env, regexp, ou_code,work_hour_date_range))
    print(dict(zip(regexp,ou_code)))
    
        #  creator
        # ,updater
        # ,deleter
        # ,
    sql = """
    SELECT create_time
        ,update_time
        ,delete_time
        ,update_date
        ,origin_receive
        ,origin_send
        ,origin_psn
        ,hongmei_receive
        ,hongmei_send
        ,hongmei_psn
        ,pingshan_receive
        ,pingshan_send
        ,pingshan_psn
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
        ,nanhua_psn
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
        , row_number() over (partition by update_date order by update_time desc) as rn
    FROM ods_public.ods_huawei_output
    WHERE 
    update_date != '' """
    # and inc_day >= '""" + start_date + "'" 

    # sql2 = """
    # select  
    #     *
    #     from
    #     dsc_dwd.dwd_dsc_huawei_working_hour_dtl_di
    #     where
    #     inc_day >= '""" + work_hour_date_range + """'"""
    
    print(sql)
    huawei_output = spark.sql(sql).select("*").toPandas()


    # 20220110
    huawei_output = huawei_output[huawei_output['rn'].astype(int) == 1]
    
    # 20220110

    print("==================================read_table%s================================"%env)
    print(huawei_output.head())

    def datetime_(coach, col):
        """
        detail time to date; 
        split and assign to 3 cols of year m d
        """
        coach[col] = coach[col].astype(str).str.slice(0,10)
        # 20220110
        coach = pd.concat([coach.reset_index(drop = True), pd.DataFrame(list(coach[col].str.split('-')))], axis =1) 
        coach = coach[coach[[0,1,2]].fillna(0).astype(int).sum(axis = 1) != 0] # 年月日三列
        coach = coach.rename({0:'year', 1:'month', 2: 'date'}, axis=1)
        return coach

    huawei_output = datetime_(
        coach=huawei_output, col='update_date'
        ).drop(['create_time', 'update_time'], axis = 1).drop_duplicates().sort_values('update_date')
   
    """
    drop useless cols
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
    
    relist = [
        'origi', 
        'hon',  # 20220119 to hongmei
        'pearl', 
        'guiy', 
        '^t\_', 
        'r4\_', 
        'nanh', 
        'ansh', 
        'te\_',
        'pingsha' # 20220119 add
      
    ] # origi,hon,pearl,guiy,^t\_,r4\_,nanh,ansh,te\_,pingsha

    oulist = [
        'HUAWEDHW4S',
        'HONORDGHMS',
        'HUAWEDHWTS',
        'HUAWEDGTRD',
        'HUAWEDGLSS',
        'HUAWEDHW1S',
        'HUAWEDGNHS',
        'NEXPEDGWHS',
        'TYCOTSDXXS',
        'HONORSZIHS',  # 20220119 add
    ] # HUAWEDHW4S,HONORDGHMS,HUAWEDHWTS,HUAWEDGTRD,HUAWEDGLSS,HUAWEDHW1S,HUAWEDGNHS,NEXPEDGWHS,TYCOTSDXXS,HONORSZIHS

    relist = regexp
    oulist = ou_code
    
    # del dict 
    # this is a test message
    my_dict = dict(zip(relist, oulist))
    print(my_dict)

    """
    te 站点的modify, 将 te_origin 和 te_product 合并相加.
    """
    huawei_output[[
        'origin_receive'
        ,'origin_send'
        ,'origin_psn'
        ,'hongmei_receive'
        ,'hongmei_send'
        ,'hongmei_psn'
        ,'pingshan_receive'
        ,'pingshan_send'
        ,'pingshan_psn'
        ,'pearlriver_transport_times'
        ,'guiyang_transport_times'
        ,'t_product_receive'
        ,'t_product_clean'
        ,'t_product_send'
        ,'r4_receive'
        ,'r4_send'
        ,'r4_psn'
        ,'nanhua_receive'
        ,'nanhua_send'
        ,'nanhua_psn'
        ,'anshi_receive'
        ,'anshi_send'
        ,'anshi_case'
        ,'te_origin_receive'
        ,'te_origin_send'
        ,'te_product_receive'
        ,'te_product_send']] == huawei_output[[
            'origin_receive'
            ,'origin_send'
            ,'origin_psn'
            ,'hongmei_receive'
            ,'hongmei_send'
            ,'hongmei_psn'
            ,'pingshan_receive'
            ,'pingshan_send'
            ,'pingshan_psn'
            ,'pearlriver_transport_times'
            ,'guiyang_transport_times'
            ,'t_product_receive'
            ,'t_product_clean'
            ,'t_product_send'
            ,'r4_receive'
            ,'r4_send'
            ,'r4_psn'
            ,'nanhua_receive'
            ,'nanhua_send'
            ,'nanhua_psn'
            ,'anshi_receive'
            ,'anshi_send'
            ,'anshi_case'
            ,'te_origin_receive'
            ,'te_origin_send'
            ,'te_product_receive'
            ,'te_product_send']].fillna('0').astype(int)
    print("===============================debug_print1================================")
    print(huawei_output.info())
    print("===============================debug_print2================================")
    print(huawei_output.query("update_date == '2022-02-13'")[['r4_receive', 'nanhua_receive']].sum(axis = 1))

    # huawei_output['te_product_receive'] = huawei_output[['te_origin_receive', 'te_product_receive']].fillna(0).astype(int).sum(axis = 1)
    # huawei_output['te_product_send'] = huawei_output[['te_origin_send', 'te_product_send']].fillna(0).astype(int).sum(axis = 1)
    
    # 2022.02.15 r4(华技) 和 南华合并.
    
    huawei_output21 = huawei_output[huawei_output['update_date'] < '2022-01-01']
    huawei_output22 = huawei_output[huawei_output['update_date'] >= '2022-01-01']

    huawei_output22['r4_receive'] = huawei_output22[['r4_receive', 'nanhua_receive']].fillna(0).astype(int).sum(axis = 1)
    huawei_output22['r4_send'] = huawei_output22[['r4_send', 'nanhua_send']].fillna(0).astype(int).sum(axis = 1)
    huawei_output22['r4_psn'] = huawei_output22[['r4_psn', 'nanhua_psn']].fillna(0).astype(int).sum(axis = 1)
    
    huawei_output22[['nanhua_receive', 'nanhua_send', 'nanhua_psn']] = 0
    huawei_output = pd.concat([huawei_output21,huawei_output22], axis = 0)
    # print(huawei_output['te_origin_receive'].dtype)

    print(huawei_output.query("update_date == '2022-01-01'")[['te_origin_receive','te_origin_send'] ])
    huawei_output = huawei_output.drop(['te_origin_send', 'te_origin_receive'], axis = 1)


    print("===============================before_concat================================")
    print(huawei_output.columns)
    # %%
    def concat_(re, ou):
        """
        h合并表格并且对额外列变换为addition
        """
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
        df['update_date'] = df['update_date'].astype(str).str.replace('\-', '') # 统一格式 20220125
        df['addition_type'] = df['addition_type'].str.replace('^z', 'None')
        return df
    df = cleanm(df)
    df['inc_day'] = datetime.now().strftime('%Y%m%d')

    print("===============================data_prepared%s================================"%start_date)
    df[['receive', 'send', 'psn', 'transport_times', 'addition']] = df[
        ['receive', 'send', 'psn', 'transport_times', 'addition']
        ].astype(int)

    print(df.info())
    df = df.fillna(0)
    print(df.head())
    df = df[[
        'update_date',
        'ou',
        'receive',
        'send',
        'psn',
        'transport_times',
        'station',
        'addition_type',
        'addition',
        'inc_day',
        ]]
        

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
    print("===============================merge_table--%s================================="%merge_table)

    sql = """insert overwrite table """ + merge_table +  """ select * from df"""
    # print(sql)
    spark.sql(sql).show()
    # inc_df = spark.sql("select * from df")
    # # (table_name, df, pk_cols, order_cols, partition_cols=None):
    # merge_data = MergeDFToTable(merge_table, inc_df, \
    #     "ou, update_date, inc_day", "update_date")
    # merge_data.merge()



def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')], nargs="*")
    args.add_argument("--env", help="dev environment or prod environment", default=["dev"], nargs="*")

    args.add_argument("--regexp", help="regexp in dictionary", default=[
        'origi', 
        'hon', 
        'pearl', 
        'guiy', 
        '^t\_', 
        'r4\_', 
        'nanh', 
        'ansh', 
        'te\_',
        'pingsha'], nargs="*")

    args.add_argument("--ou_code", help="dev environment or prod environment", default=[
        'HUAWEDHW4S',
        'HONORDGHMS',
        'HUAWEDHWTS',
        'HUAWEDGTRD',
        'HUAWEDGLSS',
        'HUAWEDHW1S',
        'HUAWEDGNHS',
        'NEXPEDGWHS',
        'TYCOTSDXXS',
        'HONORSZIHS'], nargs="*")
    args.add_argument("--work_hour_date_range", help="date range u want to refresh in format of yyyyMMdd", default=[(datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')], nargs="*")



        

    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    env = args_parse.env[0]

    regexp = args_parse.regexp
    ou_code = args_parse.ou_code
    work_hour_date_range = args_parse.work_hour_date_range[0]
    
    run_etl(start_date, env, regexp, ou_code, work_hour_date_range)

    
if __name__ == '__main__':
    main()

    

 