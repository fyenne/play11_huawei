SELECT  creator
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
WHERE inc_day > '20211127' ;

SELECT  deleter
       ,createtime
       ,updatetime
       ,deletetime
       ,update_person
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
       ,id
       ,appid
       ,entryid
       ,creator_id
       ,creator_name
       ,creator_username
       ,creator_status
       ,updater_id
       ,updater_name
       ,updater_username
       ,updater_status
       ,creator
       ,updater
FROM ods_public.huawei_daliy_operation
WHERE inc_day > '20211127'




drop table dsc_dws.dws_dsc_huawei_operation_sum_df

CREATE TABLE `dsc_dws.dws_dsc_huawei_operation_sum_df`(
`update_date` string COMMENT '更新日期',
`ou` string COMMENT 'ou_code',
`receive` int COMMENT '收货件数',
`send` int COMMENT '发货件数',
`psn` int COMMENT '贴标件数',
`transport_times` int COMMENT '运输趟数',
`station` string COMMENT '站点名称',
`addition_type` string COMMENT '额外操作类型',
`addition` int COMMENT '额外操作数量',
`inbound_wh` double comment '相应动作总工作时长' ,
`outbound_wh` double comment '相应动作总工作时长' ,
`psn_wh` double comment '相应动作总工作时长' ,
`add_wh` double comment '相应动作总工作时长' ,
`inc_day` string COMMENT '更新日期')
COMMENT 'dws_dsc_huawei_operation_sum_df'
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 




---

/* huawei mapping */

 
insert overwrite table  dsc_dim.dim_dsc_huawei_os_name_list_rel
select
  emp_no,
  emp_name,
  site_name,
  cost_center,
  dept,
  emp_type,
  mapping_no,
  eff_index_name,
  inc_day as update_date
from
  (
    SELECT
      a.emp_no,
      a.emp_name,
      a.site_name,
      a.cost_center,
      a.dept,
      a.emp_type,
      a.inc_day,
      row_number() over(
        partition by emp_no,
        emp_name,
        site_name,
        cost_center
      ) as rn1,
      b.mapping_no,
      b.eff_index_name,
      b.rn as rn2
    FROM
      ods_public.ods_huawei_outsourcing_name_list a
      left join (
        SELECT
          mapping_no,
          site,
          dept,
          eff_index_name,
          ou_code,
          row_number() over(
            partition by mapping_no,
            site,
            dept,
            eff_index_name,
            ou_code
            order by
              update_time desc
          ) as rn
        FROM
          ods_public.ods_huawei_opt_dept_mapping
        where
          inc_day = '$[time(yyyyMMdd,-1d)]'
      ) b on a.site_name = b.site
      and a.cost_center = b.ou_code
      and a.dept = b.dept
    where
      a.inc_day = '$[time(yyyyMMdd,-1d)]'
  ) out1
where
  rn1 = 1
  and rn2 = 1
 




--- 工时

 SELECT  a.*
       ,b.working_hours
       ,b.operation_day
FROM dsc_dim.dim_dsc_huawei_os_name_list_rel a
LEFT JOIN
(
	SELECT  emp_code
	       ,emp_name
	       ,working_hours
	       ,regexp_replace( regexp_replace(working_date,'\s\d+\:.+',''),'\-','' ) operation_day
	       ,cost_center
	FROM dsc_dwd.dwd_hr_dsc_working_hour_dtl_di
	WHERE inc_day = '$[time(yyyyMMdd,-1d)]'
) b
ON a.emp_no = b.emp_code AND a.cost_center = b.cost_center
WHERE coalesce(operation_day, '') != '' 


  




/* power bi , fin */
 -- ads_dsc_huawei_work_efficiency_sum_df
 
 insert overwrite table dm_dsc_ads.ads_dsc_huawei_work_efficiency_sum_df  
select 
a.update_date,
a.ou,
b.site_name,
a.receive,
a.send,
a.psn,
a.transport_times,
a.addition_type,
a.addition,
b.total_working_hours_PSN,
b.mean_working_hours_PSN,
b.emp_no_PSN,
b.total_working_hours_received,
b.mean_working_hours_received,
b.emp_no_received,
b.total_working_hours_sent,
b.mean_working_hours_sent,
b.emp_no_sent,
b.total_working_hours_transport_times,
b.mean_working_hours_transport_times,
b.emp_no_transport_times ,
a.receive / b.total_working_hours_received as receive_eff,
a.send / b.total_working_hours_sent as send_eff,
a.psn / b.total_working_hours_PSN as psn_eff,
a.transport_times / b.total_working_hours_transport_times as trans_eff

from dsc_dws.dws_dsc_huawei_operation_sum_df as a
left join dsc_dws.dws_dsc_huawei_work_hour_sum_df as b
on 
regexp_replace(a.update_date, '\-', '') = b.update_date
and
a.ou = b.cost_center 
  



  gerry.huang@sf-dsc.com	HUAWEDHW4S	HONORDGHMS	HUAWEDHWTS	HUAWEDGTRD	HUAWEDGLSS	HUAWEDHW1S	HUAWEDGNHS	NEXPEDGWHS	TYCOTSDXXS	HONORSZIHS