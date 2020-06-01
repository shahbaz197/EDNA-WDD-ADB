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
val wd_rawdataframe_cfr=spark.read.option("multiLine", true).json(s"abfss://edna-workforcedatadomain@bdaze1iednadl01.dfs.core.windows.net/Raw/Workday/landing_workday_files/${source_filename_wd}")
//rawextract.printSchema()
import org.apache.spark.sql.functions
wd_rawdataframe_cfr.withColumn("Emp_data",explode($"Report_Entry")).createOrReplaceTempView("Employee_report_entry")

import org.apache.spark.sql.functions
val wd_flattendataframe_cfr = wd_rawdataframe_cfr.withColumn("Emp_data",explode($"Report_Entry")).withColumn("Load_Datetime",lit(current_timestamp())).withColumn("sourcefile", substring_index(input_file_name(), "/", -1))   //.withColumn("Emp_data",explode($"Report_Entry"))
//final_df_temp2.printSchema()
//final_df_temp2.show()
//wd_flattendataframe.createOrReplaceTempView("employee_workday")
//wd_flattendataframe.printSchema()
val wd_flattendataframe_temp =spark.sql("select emp_data.* from Employee_report_entry")

// COMMAND ----------

val workday_dataframe_cfr=wd_flattendataframe_cfr.select(
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
col("Emp_data.Worker_Sub-Type").as("Worker_Sub_Type")).createOrReplaceTempView("df_employee_wd_cfr")
//val workday_dataframe_tbl_cfr=spark.sql("select * from df_employee_wd_cfr")//.persist(StorageLevel.MEMORY_AND_DISK)


// COMMAND ----------

spark.sql("""select
Employee_ID
,case when upper(First_Name)='''NULL''' then  null  else First_Name End as First_Name
,case when upper(Middle_Name)='''NULL''' then  null  else Middle_Name End as Middle_Name
,case when upper(Last_Name)='''NULL''' then  null  else Last_Name End as Last_Name
,case when upper(Preferred_Name_First_Name)='''NULL''' then  null  else Preferred_Name_First_Name End as Preferred_Name_First_Name
,case when upper(Preferred_Name_Middle_Name)='''NULL''' then  null  else Preferred_Name_Middle_Name End as Preferred_Name_Middle_Name
,case when upper(User_name)='''NULL''' then  null  else User_name End as User_name
,case when upper(Preferred_Name_Last_Name)='''NULL''' then  null  else Preferred_Name_Last_Name End as Preferred_Name_Last_Name
,case when upper(Phone_Number)='''NULL''' then  null  else Phone_Number End as Phone_Number
,case when upper(Home_Phone_Number)='''NULL''' then  null  else Home_Phone_Number End as Home_Phone_Number
,case when upper(primary_Work_Email)='''NULL''' then  null  else primary_Work_Email End as primary_Work_Email
,case when upper(Location)='''NULL''' then  null  else Location End as Location
,case when upper(Location_Address_Country)='''NULL''' then  null  else Location_Address_Country End as Location_Address_Country
,case when upper(Location_Reference_ID)='''NULL''' then  null  else Location_Reference_ID End as Location_Reference_ID
,case when upper(Position_ID)='''NULL''' then  null  else Position_ID End as Position_ID
,case when upper(Position_Title)='''NULL''' then  null  else Position_Title End as Position_Title
,case when upper(Previous_System_ID)='''NULL''' then  null  else Previous_System_ID End as Previous_System_ID
,case when upper(Is_Manager)='''NULL''' then  null  else Is_Manager End as Is_Manager
,case when upper(Hire_Date)='''NULL''' then  null  else Hire_Date End as Hire_Date
,case when upper(Original_Hire_Date)='''NULL''' then  null  else Original_Hire_Date End as Original_Hire_Date
,case when upper(Job_Classification_ID)='''NULL''' then  null  else Job_Classification_ID End as Job_Classification_ID
,case when upper(Job_Code)='''NULL''' then  null  else Job_Code End as Job_Code
,case when upper(Job_Family)='''NULL''' then  null  else Job_Family End as Job_Family
,case when upper(Job_Family_Group)='''NULL''' then  null  else Job_Family_Group End as Job_Family_Group
,case when upper(Active_Status)='''NULL''' then  null  else Active_Status End as Active_Status
,case when upper(Contract_End_Date)='''NULL''' then  null  else Contract_End_Date End as Contract_End_Date
,case when upper(Manager_Employee_ID)='''NULL''' then  null  else Manager_Employee_ID End as Manager_Employee_ID
,case when upper(Supervisory_Org_Name)='''NULL''' then  null  else Supervisory_Org_Name End as Supervisory_Org_Name
,case when upper(Supervisory_Organization_ID)='''NULL''' then  null  else Supervisory_Organization_ID End as Supervisory_Organization_ID
,case when upper(Cost_Center_ID)='''NULL''' then  null  else Cost_Center_ID End as Cost_Center_ID
,case when upper(Cost_Centre)='''NULL''' then  null  else Cost_Centre End as Cost_Centre
,case when upper(Cost_Center_Name)='''NULL''' then  null  else Cost_Center_Name End as Cost_Center_Name
,case when upper(Contingent_Worker_Supplier)='''NULL''' then  null  else Contingent_Worker_Supplier End as Contingent_Worker_Supplier
,case when upper(Time_Type)='''NULL''' then  null  else Time_Type End as Time_Type
,case when upper(Worker_Type)='''NULL''' then  null  else Worker_Type End as Worker_Type
,case when upper(Worker_Sub_Type)='''NULL''' then  null  else Worker_Sub_Type End as Worker_Sub_Type
from df_employee_wd_cfr""").createOrReplaceTempView("checkfornulls_tbl")

// COMMAND ----------

val checkfornulls=spark.sql("select * from checkfornulls_tbl")

// COMMAND ----------

val aftercoalesce=checkfornulls.withColumn("Record_status",coalesce($"First_Name",$"Middle_Name",$"Last_Name",$"Preferred_Name_First_Name",$"Preferred_Name_Middle_Name",$"User_name",$"Preferred_Name_Last_Name",$"Phone_Number",$"Home_Phone_Number",$"primary_Work_Email",$"Location",$"Location_Address_Country",$"Location_Reference_ID",$"Position_ID",$"Position_Title",$"Previous_System_ID",$"Is_Manager",$"Hire_Date",$"Original_Hire_Date",$"Job_Classification_ID",$"Job_Code",$"Job_Family",$"Job_Family_Group",$"Active_Status",$"Contract_End_Date",$"Manager_Employee_ID",$"Supervisory_Org_Name",$"Supervisory_Organization_ID",$"Cost_Center_ID",$"Cost_Centre",$"Cost_Center_Name",$"Contingent_Worker_Supplier",$"Time_Type",$"Worker_Type",$"Worker_Sub_Type",lit("No_Record")))
aftercoalesce.show()

// COMMAND ----------

val valid_data = aftercoalesce.filter($"Record_status" =!= "No_Record")

// COMMAND ----------

valid_data.count

// COMMAND ----------

// DBTITLE 1,Assign Proper column names
val workday_dataframe_cfr=wd_flattendataframe_cfr.select(
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
col("Emp_data.Worker_Sub-Type").as("Worker_Sub_Type")
//col("Load_Datetime").as("Load_Datetime"),
//col("sourcefile").as("sourcefile")).withColumn("datepart",lit(current_date())
                                              ).createOrReplaceTempView("df_employee_wd_cfr")
//workday_dataframe.show()
val workday_dataframe_tbl_cfr=spark.sql("select count(*) from df_employee_wd_cfr")//.persist(StorageLevel.MEMORY_AND_DISK)
//workday_dataframe_tbl.persist()

// COMMAND ----------

// DBTITLE 1,Control process based on conditions
//if(workday_dataframe_tbl_cfr.head(1)(0)(0)==0 || workday_dataframe_tbl_cfr.rdd.isEmpty() || wd_flattendataframe_temp.columns.size<=1 || valid_data.count > 0) dbutils.notebook.exit("stop") else  dbutils.notebook.exit("proceed")

// COMMAND ----------

if(valid_data.count > 0)  dbutils.notebook.exit("proceed") else dbutils.notebook.exit("stop") 