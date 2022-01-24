# %%
import pandas as pd 
import numpy as np 
import os 
import warnings
from functools import reduce

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



def run_etl(start_date, env):
    
    print("python version here:", sys.version, '\t') 
    print("===================================sysVersion================================")
 
    print("my parameters",  print(env, start_date))
 
  
    sql = """
    select  
        *
        from
        dsc_dwd.dwd_dsc_huawei_working_hour_dtl_di
        where
        emp_type != '正式工' """

        # and inc_day <= '""" + start_date + """'    不要日期筛选了, 现在全量,.
    print(sql)
    df2 = spark.sql(sql).select("*").toPandas()
    # df2 = df2[df2['rn'].astype(int) == 1]
    print(df2.info())
    # 20220110
    df2['working_hours'] = df2['working_hours'].fillna(0).astype(float)

    print("==================================read_table%s================================"%env)
    # %%
    # from fyenn_class import  pd_loaddata, pd 
    # df2 = pd_loaddata.pd_csv(path+'dwd_dsc_huawei_working_hour_dtl_di.csv')
    # df2 = pd_loaddata.bdp_col(df2)
    
    df3 = df2.groupby(['cost_center', 'mapping_no','update_date', 'site_name']).agg(
        {'working_hours':['sum', 'mean'], 'emp_no' : ['nunique']} 
    )
    df3.columns = df3.columns.get_level_values(0)
    df3 = df3.reset_index()
    df3.columns = ['cost_center', 'mapping_no', 'update_date', 'site_name','total_working_hours',
        'mean_working_hours', 'emp_no']
    df3['mapping_no'] = df3['mapping_no'].str.replace('——', '-')
    df3 = df3[df3['update_date'].apply(lambda x: len(x)) == 8]

    types = ['(PS)', '(.+收货)', '(.+发货)', '(.+趟)']
    transla = ['PSN', 'received', 'sent', 'transport_times']
  
    dicts = dict(zip(types, transla))
    df_out = list()
    for i in ['(PS)', '(.+收货)', '(.+发货)', '(.+趟)']:
        df_mid = df3[df3['mapping_no'].str.match(i)]
        df_mid['mapping_no'] = pd.Series(df_mid['mapping_no'].unique()).str.replace(i + '.+', dicts[i])[0]
        df_mid.columns = ['cost_center', 'mapping_no', 'update_date', 'site_name'] + [j + '_' +  df_mid['mapping_no'].str.extract("(" + dicts[i] + ")").iloc[0,0] for j in list(df_mid.columns)[-3:] ]
        df_mid = df_mid.drop('mapping_no', axis = 1)
        df_out.append(df_mid)
    
    data_frames = df_out
    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['cost_center', 'update_date', 'site_name'],
                                                how='outer'), data_frames)

    # %%

    df = df_merged[[
        'cost_center',
        'site_name',
        'update_date',
        'total_working_hours_PSN',
        'mean_working_hours_PSN',
        'emp_no_PSN',
        'total_working_hours_received',
        'mean_working_hours_received',
        'emp_no_received',
        'total_working_hours_sent',
        'mean_working_hours_sent',
        'emp_no_sent',
        'total_working_hours_transport_times',
        'mean_working_hours_transport_times',
        'emp_no_transport_times'
        ]]


    print("===============================data_prepared \s %s================================"%start_date)
    df[[ 
        'total_working_hours_PSN',
        'mean_working_hours_PSN',
        'emp_no_PSN',
        'total_working_hours_received',
        'mean_working_hours_received',
        'emp_no_received',
        'total_working_hours_sent',
        'mean_working_hours_sent',
        'emp_no_sent',
        'total_working_hours_transport_times',
        'mean_working_hours_transport_times',
        'emp_no_transport_times'
        ]] = df[[ 
        'total_working_hours_PSN',
        'mean_working_hours_PSN',
        'emp_no_PSN',
        'total_working_hours_received',
        'mean_working_hours_received',
        'emp_no_received',
        'total_working_hours_sent',
        'mean_working_hours_sent',
        'emp_no_sent',
        'total_working_hours_transport_times',
        'mean_working_hours_transport_times',
        'emp_no_transport_times'
        ]].fillna(0).astype(float)

    print(df.info())
    print(df.head())
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
   

    merge_table = "dsc_dws.dws_dsc_huawei_work_hour_sum_df"
    if env == 'dev':
        merge_table = 'tmp_' + merge_table
    else:
        pass
    print('看一下merge_table from john')
    print("===============================merge_table--%s================================="%merge_table)

    sql = """insert overwrite table """ + merge_table +  """ select * from df"""
    print(sql)
    spark.sql(sql).show()

    # spark.sql("""set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict""")
    # # (table_name, df, pk_cols, order_cols, partition_cols=None):
    # merge_data = MergeDFToTable(merge_table, inc_df, \
    #     "ou, update_date, inc_day", "inc_day", partition_cols="inc_day")
    # merge_data.merge()



def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')], nargs="*")
    args.add_argument("--env", help="dev environment or prod environment", default=["dev"], nargs="*")
  

        

    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    env = args_parse.env[0]
 
    
    run_etl(start_date, env)

    
if __name__ == '__main__':
    main()

    

 