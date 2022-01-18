#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import jdy
import pgOperation


# In[3]:


def getFullJdyDf(APP_ID, ENTRY_ID, API_KEY):
    api = jdy.APIUtils(APP_ID, ENTRY_ID, API_KEY)
    form_data = api.get_all_data([], {})
    df3 = pd.json_normalize(form_data)
    return df3


# In[7]:

def writePgFromApi(pg,APP_ID, ENTRY_ID, API_KEY, table_name):
    df = getFullJdyDf(APP_ID, ENTRY_ID, API_KEY)
    if table_name == 'huawei_daliy_operation':
        df.columns = ["deleter","create_time","update_time","delete_time","update_person","update_date","origin_receive","origin_send","origin_psn","honor_receive","honer_send","honor_psn","pearlriver_transport_times","guiyang_transport_times","t_product_receive","t_product_clean","t_product_send","r4_receive","r4_send","r4_psn","nanhua_receive","nanhua_send","nanhau_psn","anshi_receive","anshi_send","anshi_case","te_origin_receive","te_origin_send","te_product_receive","te_product_send","pre_receive","pre_withdraw","honor_transport_times","id","app_id","entry_id","creator_id","creator_name","creator_username","creator_status","updater_id","updater_name","updater_username","updater_status","creator","updater"]
    elif table_name == 'huawei_output':
        df.columns = ["creator","updater","deleter","create_time", "update_time", "delete_time", "update_date","origin_receive","origin_send","origin_psn","honor_receive","honer_send","honor_psn","pearlriver_transport_times","guiyang_transport_times","t_product_receive","t_product_clean","t_product_send","r4_receive","r4_send","r4_psn","nanhua_receive","nanhua_send","nanhau_psn","anshi_receive","anshi_send","anshi_case","te_origin_receive","te_origin_send","te_product_receive","te_product_send","pre_receive","pre_withdraw","honor_transport_times","id","app_id","entry_id"]
    elif table_name == 'huawei_outsourcing_name_list':
        df.columns = ["deleter","create_time","update_time","delete_time", 'area',
        'out_source_company', 'company', 'emp_no', 'emp_name', 'city',
        'job_name', 'site_name', 'cost_center', 'entry_date', 'leave_date',
        'is_rehired', 'is_cancel_entry', 'salary_amount', 'job_allowance',
        'meal_allowance', 'traffic_allowance', 'accom_allowance',
        'phone_allowance', 'social_insurance_allowance', 'level_allowance',
        'skill_allowance', 'env_allowance', 'special_allowance',
        'seniority_allowance', 'guaranteed_bonus', 'social_insurance_cost',
        'out_source_service_cost', 'package_salary', 'charged_hr',
        'update_time', 'comment', 'id_no', 'dept', 'by_time_or_piece',
        'emp_type', "id","app_id","entry_id","creator_id","creator_name",
        "creator_username","creator_status","updater_id","updater_name",
        "updater_username","updater_status"]
    elif table_name == 'huawei_opt_dept_mapping':
        df.columns = ["deleter","create_time","update_time","delete_time", 'flow_state',
        'table_id', 'mapping_no', 'site', 'dept', 'eff_index_name', 'ou_code',
        "id","app_id","entry_id","creator_id","creator_name",
        "creator_username","creator_status","updater_id","updater_name",
        "updater_username","updater_status"]

    pg.writeDfToPg(df,table_name)
    pg.grantAllOn(table_name,'smart')

# In[5]:


def main():
    pg = pgOperation.PgOperation('public') # schema 自行配置
    # 每日操作更新
    APP_ID_1 = '617643f8308c2f000941f21b'
    ENTRY_ID_1 = '617915c10613da0007c9a4d4'
    API_KEY_1 = 'nqLEWktb51m3oYbYOZa5a2Ai6Lp8BHT7'
    writePgFromApi(pg,APP_ID_1, ENTRY_ID_1, API_KEY_1,'huawei_daliy_operation')

    # 输出
    APP_ID_2 = '617643f8308c2f000941f21b'
    ENTRY_ID_2 = '618a1c06a584c2000707be7f'
    API_KEY_2 = 'nqLEWktb51m3oYbYOZa5a2Ai6Lp8BHT7'
    writePgFromApi(pg,APP_ID_2, ENTRY_ID_2, API_KEY_2,'huawei_output')

    #外包人员名单
    APP_ID_3 = '617643f8308c2f000941f21b'
    ENTRY_ID_3 = '617fc35d7ffed400078e5b08'
    API_KEY_3 = 'nqLEWktb51m3oYbYOZa5a2Ai6Lp8BHT7'
    writePgFromApi(pg,APP_ID_3, ENTRY_ID_3, API_KEY_3,'huawei_outsourcing_name_list')

    #业务量与部门关系匹配表
    APP_ID_4 = '617643f8308c2f000941f21b'
    ENTRY_ID_4 = '618117cd721fc60008a31bd3'
    API_KEY_4 = 'nqLEWktb51m3oYbYOZa5a2Ai6Lp8BHT7'
    writePgFromApi(pg,APP_ID_4, ENTRY_ID_4, API_KEY_4,'huawei_opt_dept_mapping')

# In[8]:


if __name__ == "__main__":
    main()
