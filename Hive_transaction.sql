-- Databricks notebook source
set hive.support.concurrency = true;
set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on = true;
set hive.compactor.worker.threads = 1;

-- COMMAND ----------

set hive.support.concurrency = true;

-- COMMAND ----------

set hive.enforce.bucketing = true;

-- COMMAND ----------

set hive.exec.dynamic.partition.mode = nonstrict;

-- COMMAND ----------

set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- COMMAND ----------

set hive.compactor.initiator.on = true;

-- COMMAND ----------

CREATE TABLE college(clg_id int,clg_name string,clg_loc string) clustered by (clg_id) into 5 buckets stored as orc TBLPROPERTIES('transactional'='true');

-- COMMAND ----------

INSERT INTO table college values(1,'nec','nlr'),(2,'vit','vlr'),(3,'srm','chen'),(4,'lpu','del'),(5,'stanford','uk'),(6,'JNTUA','atp'),(7,'cambridge','us');

-- COMMAND ----------

select * from college

-- COMMAND ----------

UPDATE college set clg_id = 8 where clg_id = 7;
