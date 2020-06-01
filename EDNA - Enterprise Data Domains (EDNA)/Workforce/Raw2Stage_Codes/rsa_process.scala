// Databricks notebook source
// DBTITLE 1,Declare Variables
//Declaring variables
val adls_storage_account_name = "bdaze1iednadl01"
val adls_container_name = "edna-workforcedatadomain" 
val cntrl_table = "workforce_cntrl_mdt.rsa_r2s_cntrl"
val path_cntrl_table = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/Control_Metadata/rsa_r2s_cntrl"
val source_table= "workforce_raw.employee_rsa"
val business_name="RSA"
val rsa_employee_raw= "workforce_raw.rsa_employee"
val rsa_employee= "workforce_stage.rsa_employee_stg"
val rsa_employee_inc= "workforce_stage.rsa_employee_inc"
//	AKV_EDNA
/*
val pswd=dbutils.secrets.get(scope = "adls_workforce_accesskey", key = "DataOpsDW-elancoadmin")
val key=dbutils.secrets.get(scope = "adls_workforce_accesskey", key = "ADLS-WORKFORCEDATADOMAIN")
val secret=dbutils.secrets.get(scope = "db_workforce_scope", key = "DataOpsServicePrincipal01-Secret")
val clintid=dbutils.secrets.get(scope = "db_workforce_scope", key = "DataOpsServicePrincipal01-ClintID")
val tenantid=dbutils.secrets.get(scope = "db_workforce_scope", key = "DataOpsServicePrincipal01-TenantID")
*/
val pswd=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsDW-elancoadmin")
val key=dbutils.secrets.get(scope = "AKV_EDNA", key = "ADLS-WORKFORCEDATADOMAIN")
val secret=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-ClintID")
val tenantid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-TenantID")

// COMMAND ----------

// DBTITLE 1,Configure source(ADLS) 
dbutils.widgets.text("valsource_filename_RSA", "RSA_DATA_2020-04-08_125005")
val source_filename_rsa = dbutils.widgets.get("valsource_filename_RSA")
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.util.matching.Regex
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.to_timestamp

def removeAllWhitespace(col: Column): Column = {
  regexp_replace(col,"\\s+","")
}

// COMMAND ----------

// DBTITLE 1,Configure source(ADLS) - hadoop conf
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

// DBTITLE 1,create Dataframe
val rsa_dataframe = spark.read.format("avro").load(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/Raw/RSA/landing_rsa_files/${source_filename_rsa}")
rsa_dataframe.
select("ID",
"USER_ID",
"IDC_ID",
"FIRST_NAME",
"MIDDLENAME",
"LAST_NAME",
"PREFERREDFIRSTNAME",
"PREFERREDMIDDLENAME",
"PREFERREDLASTNAME",
"PHONE",
"EMAIL_ADDRESS",
"LILYEMAIL",
"LOCATION",
"ADDRESS",
"DEPARTMENT",
"TITLE",
"BUSINESS_UNIT_ID",
"CONTINGENTWORKERTYPE",
"JML_STATUS",
"JOB_STATUS",
"AVAILABILITY_STATUS",
"HIRE_DATE",
"TERMINATION_DATE",
"IS_TERMINATED",
"CREATION_DATE",
"DELETION_DATE",
"SAMACCOUNTNAME",
"SUPERVISOR_ID",
"SUPERVISOR_ID_NAME",
"BACKUP_SUPERVISOR",
"BACKUP_SUPERVISOR_NAME",
"EXCEPTION_COUNT",
"UNIQUE_ID",
"UPN",
"ADD_STATE",
"REMOVE_STATE",
"NOTUSED",
"NOTUSED1",
"VIOLATION_COUNT",
"WORKERTYPE",
"Load_Datetime").withColumn("sourcefile",substring_index(input_file_name(), "/", -1)).withColumn("datepart",lit(current_date())).createOrReplaceTempView("df_employee_rsa")

val ds_employee_rsa_tbl = spark.sql(s"select * from df_employee_rsa")
//ds_employee_rsa_tbl.persist()
//ds_employee_rsa_tbl.show()

// COMMAND ----------

// DBTITLE 1,Load Raw table
ds_employee_rsa_tbl.write.mode(SaveMode.Append).insertInto(s"${rsa_employee_raw}")

// COMMAND ----------

// DBTITLE 1,Load stage table
ds_employee_rsa_tbl.write.mode(SaveMode.Append).insertInto(s"${rsa_employee}")

// COMMAND ----------

val val_check= spark.sql(s"select business_name as check_business_name,table_name as check_table_name, max(raw_load_tm) as max_raw_load_tm, max(load_txn_tm)as max_load_txn_tm from ${cntrl_table} where business_name='${business_name}' group by business_name,table_name")

// COMMAND ----------

val val_filter = val_check.select(col("max_raw_load_tm"))
val final_val_filter = if(!val_filter.rdd.isEmpty()) (val_filter.head(1)(0)(0)) else lit("1900-01-01 00:00:00").cast(TimestampType)
//val final_frame_inc= if (!val_filter.rdd.isEmpty()) workday_dataframe_tbl.where(col("Load_Datetime") > final_val_filter) else workday_dataframe_tbl

// COMMAND ----------

val val_check= spark.sql(s"select business_name as check_business_name,table_name as check_table_name, max(raw_load_tm) as max_raw_load_tm, max(load_txn_tm)as max_load_txn_tm from ${cntrl_table} where business_name='${business_name}' group by business_name,table_name")

// COMMAND ----------

val val_filter = val_check.select(col("max_raw_load_tm"))
val final_val_filter = if(!val_filter.rdd.isEmpty()) (val_filter.head(1)(0)(0)) else lit("1900-01-01 00:00:00").cast(TimestampType)
//val final_frame_inc= if (!val_filter.rdd.isEmpty()) workday_dataframe_tbl.where(col("Load_Datetime") > final_val_filter) else workday_dataframe_tbl

// COMMAND ----------

// DBTITLE 1,Load Final data
val final_frame=ds_employee_rsa_tbl.select(
col("ID")
,col("USER_ID")
,col("IDC_ID")
,col("FIRST_NAME")
,col("MIDDLENAME")
,col("LAST_NAME")
,col("PREFERREDFIRSTNAME")
,col("PREFERREDMIDDLENAME")
,col("PREFERREDLASTNAME")
,col("PHONE")
,col("EMAIL_ADDRESS")
,col("LILYEMAIL")
,col("LOCATION")
,col("ADDRESS")
,col("DEPARTMENT")
,col("TITLE")
,col("BUSINESS_UNIT_ID")
,col("CONTINGENTWORKERTYPE")
,col("JML_STATUS")
,col("JOB_STATUS")
,col("AVAILABILITY_STATUS")
,col("HIRE_DATE")
,col("TERMINATION_DATE")
,col("IS_TERMINATED")
,col("CREATION_DATE")
,col("DELETION_DATE")
,col("SAMACCOUNTNAME")
,col("SUPERVISOR_ID")
,col("SUPERVISOR_ID_NAME")
,col("BACKUP_SUPERVISOR")
,col("BACKUP_SUPERVISOR_NAME")
,col("EXCEPTION_COUNT")
,col("UNIQUE_ID")
,col("UPN")
,col("ADD_STATE")
,col("REMOVE_STATE")
,col("NOTUSED")
,col("NOTUSED1")
,col("VIOLATION_COUNT")
,col("WORKERTYPE")
,col("Load_Datetime")
,col("sourcefile")
,col("datepart")
)

// COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic") 
//spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

// COMMAND ----------

//employee_consolidated_dataframe_inc.write.mode("overwrite").insertInto(s"${stg_employee_consolidated_inc}") 
if (!final_frame.rdd.isEmpty()) final_frame.write.mode("overwrite").insertInto(s"${rsa_employee_inc}") else null

// COMMAND ----------

// DBTITLE 1,/Control Metadata Code
import org.apache.spark.sql.functions._
val processstage="raw"
//val max_load_date=spark.sql("select max(load_datetime) as load_date from df_employee_wd")
val cntrl_metadata_frame = if(!ds_employee_rsa_tbl.rdd.isEmpty) ds_employee_rsa_tbl.select(lit(s"${business_name}").as("business_name"),lit(s"${rsa_employee}").as("table_name"),lit(s"${processstage}").as("stage"),col("Load_Datetime").as("raw_load_tm")).withColumn("load_txn_tm",lit(current_timestamp())).createOrReplaceTempView("cntrl_metadata_table")  else null


// COMMAND ----------

val df_cntrl_mtd = if (!final_frame.rdd.isEmpty()) spark.sql("select business_name,table_name,stage, max(raw_load_tm) as raw_load_tm, load_txn_tm from cntrl_metadata_table group by  business_name, table_name,load_txn_tm,stage")
else null

// COMMAND ----------

/*
if (final_frame.rdd.isEmpty()) null
else
if(!val_filter.rdd.isEmpty()) if(max_load_date.head(1)(0)(0) == val_filter.head(1)(0)(0)) null else df_cntrl_mtd.write.format("delta").mode("append").save(s"${path_cntrl_table}")
else 
df_cntrl_mtd.write.format("delta").mode("append").save(s"${path_cntrl_table}")
*/
if (final_frame.rdd.isEmpty()) null
else
if(!val_filter.rdd.isEmpty()) if(df_cntrl_mtd.head(1)(0)(2) == val_filter.head(1)(0)(0)) null else df_cntrl_mtd.write.format("delta").mode("append").insertInto(s"${cntrl_table}")
else 
df_cntrl_mtd.write.format("delta").mode("append").insertInto(s"${cntrl_table}")

//final_frame.write.mode("overwrite").insertInto(s"${wd_employee_inc}")

// COMMAND ----------

val df_cntrl_m = spark.sql(s"select * from ${cntrl_table}")

// COMMAND ----------

// DBTITLE 1,Load data
spark.conf.set("fs.azure.account.key.bdaze1iednadl01.blob.core.windows.net",key)

spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")


spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", secret)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")


ds_employee_rsa_tbl.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://bdaze1isqdwdb01.database.windows.net:1433;database=BDAZE1ISQDWSV01;user=elancoadmin;password="+pswd+";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "EDNAWorkforce.Employee_RSA_History")
  .option("tempDir", "wasbs://"+adls_container_name+"@"+adls_storage_account_name+".blob.core.windows.net/Raw/RSA/scratchpad/")
  //.option("config" "configs")
  .mode("append")
  .save()
 