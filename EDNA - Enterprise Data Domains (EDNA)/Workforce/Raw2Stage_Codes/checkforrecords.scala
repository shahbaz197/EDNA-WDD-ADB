// Databricks notebook source
// DBTITLE 1,Declare Variables
val adls_storage_account_name = "bdaze1iednadl01"
val adls_container_name = "edna-workforcedatadomain" 
val cntrl_table = "workforce_cntrl_mdt.rsa_r2s_cntrl"
val path_cntrl_table = "abfss://edna-workforcedatadomain@bdaze1iednadl01.dfs.core.windows.net/Control_Metadata/rsa_r2s_cntrl"
val source_table= "workforce_raw.employee_workday"
val business_name="workday"
val wd_employee_raw= "workforce_raw.wd_employee_raw"
val wd_employee= "workforce_raw.wd_employee_final"
val wd_employee_inc= "workforce_raw.wd_employee_inc"
val pswd=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsDW-elancoadmin")
val key=dbutils.secrets.get(scope = "AKV_EDNA", key = "ADLS-WORKFORCEDATADOMAIN")
val secret=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-ClintID")
val tenantid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-TenantID")

// COMMAND ----------

// DBTITLE 1,Configure source(ADLS) 
dbutils.widgets.text("valsource_filename_WD", "ELAN_WFDD_Demographic_2020-04-30_063106.JSON")
val source_filename_wd = dbutils.widgets.get("valsource_filename_WD")
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
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
//spark.conf.set("fs.azure.account.key.bdaze1iednadl01.blob.core.windows.net","Fw0hRjUPtpRmjD5DoiAJfWqS6J4KZ6NTKt6UQtPhkEXIkT64CTneMvzGeLBNBWmSUtkL76y/UCxfaC5mtsNbCw==")


// COMMAND ----------

// DBTITLE 1,create Dataframe
val wd_rawdataframe=spark.read.option("multiLine", true).json(s"abfss://edna-workforcedatadomain@bdaze1iednadl01.dfs.core.windows.net/Raw/Workday/landing_workday_files/${source_filename_wd}")
//rawextract.printSchema()
import org.apache.spark.sql.functions
wd_rawdataframe.withColumn("Emp_data",explode($"Report_Entry")).createOrReplaceTempView("Employee_report_entry")

import org.apache.spark.sql.functions
val wd_flattendataframe = wd_rawdataframe.withColumn("Emp_data",explode($"Report_Entry")).withColumn("Load_Datetime",lit(current_timestamp())).withColumn("sourcefile", substring_index(input_file_name(), "/", -1))   //.withColumn("Emp_data",explode($"Report_Entry"))
//final_df_temp2.printSchema()
//final_df_temp2.show()
//wd_flattendataframe.createOrReplaceTempView("employee_workday")
//wd_flattendataframe.printSchema()
val wd_flattendataframe_temp =spark.sql("select emp_data.* from Employee_report_entry")

// COMMAND ----------

// DBTITLE 1,Assign Proper column names

val workday_dataframe=wd_flattendataframe.select(
col("Emp_data.Employee_ID").as("Employee_ID"),
col("Emp_data.User_name").as("User_name"),
col("Emp_data.First_Name").as("First_Name"),
col("Emp_data.Middle_Name").as("Middle_Name"),
col("Emp_data.Last_Name").as("Last_Name"),
col("Emp_data.Preferred_Name_-_First_Name").as("Preferred_Name_First_Name"),
col("Emp_data.Preferred_Name_-_Middle_Name").as("Preferred_Name_Middle_Name"),
col("Emp_data.Preferred_Last_Name").as("Preferred_Name_Last_Name"),
col("Emp_data.Phone_Number").as("Phone_Number"),
col("Emp_data.Home_Phone_Number").as("Home_Phone_Number"),
col("Emp_data.primary_Work_Email").as("primary_Work_Email"),
col("Emp_data.Location").as("Location"),
col("Emp_data.Location_Address_-_Country").as("Location_Address_Country"),
col("Emp_data.Location_Reference_ID").as("Location_Reference_ID"),
col("Emp_data.Position_ID").as("Position_ID"),
col("Emp_data.Position_Title").as("Position_Title"),
col("Emp_data.Previous_System_ID").as("Previous_System_ID"),
col("Emp_data.Is_Manager").as("Is_Manager"),
col("Emp_data.Hire_Date").as("Hire_Date"),
col("Emp_data.Original_Hire_Date").as("Original_Hire_Date"),
col("Emp_data.Job_Classification_ID").as("Job_Classification_ID"),
col("Emp_data.Job_Code").as("Job_Code"),
col("Emp_data.Job_Family").as("Job_Family"),
col("Emp_data.Job_Family_Group").as("Job_Family_Group"),
col("Emp_data.Active_Status").as("Active_Status"),
col("Emp_data.Contract_End_Date").as("Contract_End_Date"),
col("Emp_data.Manager_Employee_ID").as("Manager_Employee_ID"),
col("Emp_data.Supervisory_Org_Name").as("Supervisory_Org_Name"),
col("Emp_data.Supervisory_Organization_-_ID").as("Supervisory_Organization_ID"),
col("Emp_data.Cost_Center_ID").as("Cost_Center_ID"),
col("Emp_data.Cost_Center").as("Cost_Centre"),
col("Emp_data.Cost_Center_Name").as("Cost_Center_Name"),
col("Emp_data.Contingent_Worker_Supplier").as("Contingent_Worker_Supplier"),
col("Emp_data.Time_Type").as("Time_Type"),
col("Emp_data.Worker_Type").as("Worker_Type"),
col("Emp_data.Worker_Sub-Type").as("Worker_Sub_Type"),
col("Load_Datetime").as("Load_Datetime"),
col("sourcefile").as("sourcefile")).withColumn("datepart",lit(current_date())).createOrReplaceTempView("df_employee_wd")
//workday_dataframe.show()
val workday_dataframe_tbl=spark.sql("select count(*) from df_employee_wd")//.persist(StorageLevel.MEMORY_AND_DISK)
//workday_dataframe_tbl.persist()

// COMMAND ----------

// DBTITLE 1,Control process based on conditions
if(workday_dataframe_tbl.head(1)(0)(0)==0 || workday_dataframe_tbl.rdd.isEmpty() || wd_flattendataframe_temp.columns.size<=1) dbutils.notebook.exit("stop") else  dbutils.notebook.exit("proceed")