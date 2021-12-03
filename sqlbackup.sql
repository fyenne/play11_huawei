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





CREATE EXTERNAL TABLE `tmp_dsc_dws.dws_dsc_huawei_operation_sum_df`(
`update_date` string comment '',
`ou` string comment '',
`receive` int comment '',
`send` int comment '',
`psn` int comment '',
`transport_times` int comment '',
`station` string comment '',
`addition_type` string comment '',
`addition` int comment '')
COMMENT 'dws_dsc_huawei_operation_sum_df'
PARTITIONED BY (
`inc_day` string COMMENT '日分区')
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
