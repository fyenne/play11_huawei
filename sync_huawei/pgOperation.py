#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class PgOperation():

    def __init__(self, schema):
        self.schema = schema

    def readTable(self, table): # usage: read table from public
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        try:
            cur = conn.cursor()
            sql = 'SELECT * FROM %s.%s'%(self.schema,table)
            df = pd.read_sql(sql,con=conn)
            cur.close()
            return df
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def writeExcelToPg(self,filename,target_table_name,f_sheet_name=0,f_skiprows=[]):  # usage: write a excel file to pgsql public scehma
        mapping = pd.read_excel(filename,sheet_name=f_sheet_name,skiprows=f_skiprows)
        engine = create_engine('postgresql+psycopg2://bi:x65IjghRes@23.209.208.48:5432/dsc')
        connection = engine.connect()
        # pg_df.head(0).to_sql('table_name', connection, if_exists='replace',index=False) #drops old table and creates new empty table

        conn = engine.raw_connection()
        cur = conn.cursor()
        mapping.to_sql(target_table_name, connection, if_exists='replace',index=False,schema=self.schema)
        # cur.execute('ALTER TABLE public.mapping OWNER to bi')
        engine.dispose()

    def writeDfToPg(self,df,target_table_name):  # usage: write a excel file to pgsql public scehma
        engine = create_engine('postgresql+psycopg2://bi:x65IjghRes@23.209.208.48:5432/dsc')
        connection = engine.connect()
        # pg_df.head(0).to_sql('table_name', connection, if_exists='replace',index=False) #drops old table and creates new empty table

        conn = engine.raw_connection()
        cur = conn.cursor()
        df.to_sql(target_table_name, connection, if_exists='replace',index=False,schema=self.schema)
        # cur.execute('ALTER TABLE public.mapping OWNER to bi')
        engine.dispose()

    def grantAllOn(self,target_table_name,user):
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        try:
            cur = conn.cursor()
            sql = 'GRANT ALL ON TABLE %s.%s TO %s;'%(self.schema,target_table_name,user)
            cur.execute(sql)
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def appendPgTable(self,df,table): # append df to a pgsql table
        # pg_df["invoice_money_done"] = pg_df["invoice_money_done"].astype(float)
        engine = create_engine('postgresql+psycopg2://bi:x65IjghRes@23.209.208.48:5432/dsc')
        connection = engine.connect()
        # df.head(0).to_sql(table, connection, if_exists='replace',index=False) #drops old table and creates new empty table
        df.to_sql(table, connection, if_exists='append',index=False,schema=self.schema)
        connection.close()
        engine.dispose()

    def runProcedure(self,procedure):
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        try:
            cur = conn.cursor()
            sql = 'CALL %s.%s;'%(self.schema,procedure)
            cur.execute(sql)
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getBdpDfByDate(self,date):
        engine = create_engine('postgresql+psycopg2://bi:x65IjghRes@23.209.208.48:5432/dsc')
        query = "SELECT * FROM %s.ods_sap_dsc_zbseg_dsc where inc_day = '%s'"%(self.schema,date)
        df = pd.read_sql_query(query, con=engine)
        engine.dispose()
        return df

    def getBdpDfBeforeDate(self,date,limit=99999999):
        engine = create_engine('postgresql+psycopg2://bi:x65IjghRes@23.209.208.48:5432/dsc')
        query = "SELECT * FROM %s.ods_sap_dsc_zbseg_dsc where inc_day <= '%s' limit %s"%(self.schema,date,str(limit))
        df = pd.read_sql_query(query, con=engine)
        engine.dispose()
        return df

    def deleteTableData(self, table):
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        sql = 'TRUNCATE %s.%s'%(self.schema,table)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return 0

    def updateConfig(self,key,value):
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        sql = 'UPDATE '+self.schema+'.ar_config SET config_value = %s WHERE config_variable = %s'
        try:
            cur = conn.cursor()
            cur.execute(sql, (value,key))
            updated_rows = cur.rowcount
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insertConfig(self,key,value):
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        sql = 'INSERT INTO '+self.schema+'.ar_config(config_variable, config_value)VALUES (%s,%s)'
        try:
            cur = conn.cursor()
            cur.execute(sql, (key,value))
            updated_rows = cur.rowcount
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def deleteAtivityRecordByDate(self,table,date):
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        sql = 'DELETE FROM '+self.schema+'.'+table+' WHERE activity_date = %s'
        rows_deleted = 0
        try:
            cur = conn.cursor()
            cur.execute(sql, (date,))
            rows_deleted  = cur.rowcount
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return rows_deleted

    def getActivityRecordByJdyID(self,table,jdy_data_id):
        engine = create_engine('postgresql+psycopg2://bi:x65IjghRes@23.209.208.48:5432/dsc')
        query = "SELECT * FROM %s.%s where jdy_data_id = '%s'"%(self.schema,table,jdy_data_id)
        df = pd.read_sql_query(query, con=engine)
        engine.dispose()
        return df


    def writeHistoryExcelToPg(self,filename,target_table_name,f_sheet_name=0,f_skiprows=[]):  # usage: write a excel file to pgsql public scehma
        mapping = pd.read_excel(filename,sheet_name=f_sheet_name,skiprows=f_skiprows)
        engine = create_engine('postgresql+psycopg2://bi:x65IjghRes@23.209.208.48:5432/dsc')
        connection = engine.connect()
        # pg_df.head(0).to_sql('table_name', connection, if_exists='replace',index=False) #drops old table and creates new empty table

        conn = engine.raw_connection()
        cur = conn.cursor()
        mapping.to_sql(target_table_name, connection, if_exists='replace',index=False,schema=self.schema)
        # cur.execute('ALTER TABLE public.mapping OWNER to bi')
        engine.dispose()

    def insertReceiveJdyId(self,row):
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        sql = 'UPDATE '+self.schema+'.receivable_jdy ' + \
        '''
        SET jdy_data_id=%s,is_changed=0 WHERE company_code=%s and customer_number=%s and finance_year=%s and profit_center=%s and project_text=%s
        '''
        try:
            cur = conn.cursor()
            cur.execute(sql, (row['jdy_data_id'],\
            row['company_code'], row['customer_number'], row['finance_year'], row['profit_center'], row['project_text'], ))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insertEstimateJdyId(self,row):
        pg_local = ['23.209.208.48', 5432, 'bi', 'x65IjghRes', 'dsc']
        conn = psycopg2.connect(host=pg_local[0], port=pg_local[1], user=pg_local[2], password=pg_local[3], database=pg_local[4])
        sql = 'UPDATE '+self.schema+'.estimate_detail ' + \
        '''
        SET jdy_data_id=%s WHERE company_code=%s and customer_number=%s and finance_year=%s and profit_center=%s and voucher_no=%s and voucher_line_no=%s and voucher_amount=%s
        '''
        try:
            cur = conn.cursor()
            cur.execute(sql, (row['jdy_data_id'],row['company_code'], row['customer_number'], row['finance_year'], row['profit_center'], row['voucher_no'], row['voucher_line_no'], row['voucher_amount']))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)