-- Databricks notebook source
-- MAGIC %scala
-- MAGIC //spark.conf.set("fs.azure.account.key.bdaze1iednadl01.blob.core.windows.net","Fw0hRjUPtpRmjD5DoiAJfWqS6J4KZ6NTKt6UQtPhkEXIkT64CTneMvzGeLBNBWmSUtkL76y/UCxfaC5mtsNbCw==")

-- COMMAND ----------

create database if not exists workforce_cntrl_mdt;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.conf.set("fs.azure.account.auth.type.bdaze1iednadl01.dfs.core.windows.net", "OAuth")
-- MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.bdaze1iednadl01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.bdaze1iednadl01.dfs.core.windows.net", "37e102ac-adcb-4903-b247-44bc16a50b1c")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.bdaze1iednadl01.dfs.core.windows.net", dbutils.secrets.get(scope = "db_workforce_scope", key = "DataOpsServicePrincipal01-Secret"))
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.bdaze1iednadl01.dfs.core.windows.net", "https://login.microsoftonline.com/8e41bacc-baba-48d6-9fcb-708bd1208e38/oauth2/token")

-- COMMAND ----------

drop table if exists workforce_cntrl_mdt.rsa_r2s_cntrl;
CREATE TABLE workforce_cntrl_mdt.rsa_r2s_cntrl (
business_name string,
table_name string,
stage string,
raw_load_tm timestamp,
load_txn_tm timestamp
--comment string
)
USING delta
location "abfss://edna-workforcedatadomain@bdaze1iednadl01.dfs.core.windows.net/Control_Metadata/rsa_r2s_cntrl"


-- COMMAND ----------

--create database if not exists workforce_stage;

-- COMMAND ----------

--create database if not exists workforce_curated;

-- COMMAND ----------

--show databases;

-- COMMAND ----------

--show tables in workforce_raw;

-- COMMAND ----------

--show tables in workforce_stage;

-- COMMAND ----------

--show tables in workforce_curated;