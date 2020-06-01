// Databricks notebook source
// DBTITLE 1,Declare Variables
val adls_storage_account_name = "bdaze1iednadl01"
val adls_container_name = "edna-workforcedatadomain" 
val wd_employee= "workforce_stage.wd_employee_inc"
val rsa_employee= "workforce_stage.rsa_employee_inc"
val stg_employee_consolidated= "workforce_stage.stg_employee_consolidated"
val stg_employee_consolidated_inc= "workforce_stage.stg_employee_consolidated_inc"
val employee_consolidated_final="workforce_curated.employee_consolidated"
val cntrl_table = "workforce_cntrl_mdt.rsa_r2s_cntrl"
val path_cntrl_table = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/Control_Metadata/rsa_r2s_cntrl"
val business_name="consolidated"
val pswd=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsDW-elancoadmin")
val key=dbutils.secrets.get(scope = "AKV_EDNA", key = "ADLS-WORKFORCEDATADOMAIN")
val secret=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-ClintID")
val tenantid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-TenantID")
//val business_name_rsa="workday"

// COMMAND ----------

// DBTITLE 1,Configure source(ADLS)
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

//display(spark.sql("select * from  workforce_stage.wd_employee_inc where employee_id=7107"))

// COMMAND ----------

// DBTITLE 1,Configure source(ADLS) 
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
/*spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", "37e102ac-adcb-4903-b247-44bc16a50b1c")
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/8e41bacc-baba-48d6-9fcb-708bd1208e38/oauth2/token")
*/
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

// DBTITLE 1,create Dataframe
val main_employee_consolidated_dataframe= sqlContext.sql(s"select a.employee_id,a.first_name,a.middle_name,a.last_name,a.preferred_name_first_name,a.preferred_name_middle_name,a.preferred_name_last_name,a.user_name,a.phone_number,a.home_phone_number,b.address as office_address,b.email_address as email_work,a.primary_work_email,a.location,' ' as location_code,a.location_address_country,a.location_reference_id,' ' as drop_code,' ' as affiliate,' ' as hub,a.position_id,a.position_title,' ' as domain,a.previous_system_id,a.is_manager,a.hire_date,a.original_hire_date,position_title as job_title,a.job_classification_id,a.job_code,a.job_family,a.job_family_group,a.active_status,a.contract_end_date,' ' as supplier,' ' as secondary_vendor,a.manager_employee_id,a.supervisory_org_name,a.supervisory_organization_id,a.time_type,a.worker_type,a.cost_center_id,a.cost_centre,a.cost_center_name,a.worker_sub_type,' ' as contingent_worker_supplier,' ' as currently_active,' ' as retiree,b.upn,b.email_address,'Y' as active_ind,a.load_datetime from ${wd_employee} a inner join ${rsa_employee} b on a.employee_id=b.user_id")//.createOrReplaceTempView("employee_consolidated")
//employee_consolidated_dataframe.show()
//main_employee_consolidated_dataframe.persist()

// COMMAND ----------

val employee_consolidated_dataframe= main_employee_consolidated_dataframe.withColumn("datepart",lit(current_date()))

// COMMAND ----------

// DBTITLE 1,Load Stage table
employee_consolidated_dataframe.write.mode(SaveMode.Append).insertInto(s"${stg_employee_consolidated}")

// COMMAND ----------

val employee_consolidated_dataframe_inc=employee_consolidated_dataframe.select(
col("Employee_ID")
,col("First_Name")
,col("Middle_Name")
,col("Last_Name")
,col("Preferred_Name_First_Name")
,col("Preferred_Name_Middle_Name")
,col("Preferred_Name_Last_Name")
,col("User_name")
,col("Phone_Number")
,col("Home_Phone_Number")
,col("Office_Address")
,col("Email_Work")
,col("primary_Work_Email")
,col("Location")
,col("Location_Code")
,col("Location_Address_Country")
,col("Location_Reference_ID")
,col("Drop_Code")
,col("Affiliate")
,col("Hub")
,col("Position_ID")
,col("Position_Title")
,col("Domain")
,col("Previous_System_ID")
,col("Is_Manager")
,col("Hire_Date")
,col("Original_Hire_Date")
,col("Job_Title")
,col("Job_Classification_ID")
,col("Job_Code")
,col("Job_Family")
,col("Job_Family_Group")
,col("Active_Status")
,col("Contract_End_Date")
,col("Supplier")
,col("Secondary_Vendor")
,col("Manager_Employee_ID")
,col("Supervisory_Org_Name")
,col("Supervisory_Organization_ID")
,col("Time_Type")
,col("Worker_Type")
,col("Cost_Center_ID")
,col("Cost_Centre")
,col("Cost_Center_Name")
,col("Worker_Sub_Type")
,col("Contingent_Worker_Supplier")
,col("Currently_Active")
,col("Retiree")
,col("UPN")
,col("EMAIL_Address")
,col("active_Ind")
,col("Load_Datetime")
,col("datepart"))

// COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic") 

// COMMAND ----------

//employee_consolidated_dataframe.write.mode(SaveMode.Append).insertInto(s"${stg_employee_consolidated}")
//employee_consolidated_dataframe.write.format("delta").mode("append").insertInto(s"${stg_employee_consolidated}") 
employee_consolidated_dataframe_inc.write.mode("overwrite").insertInto(s"${stg_employee_consolidated_inc}") 

// COMMAND ----------

// DBTITLE 1,Load WD data to Stage ADW(synapse)
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

main_employee_consolidated_dataframe.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://bdaze1isqdwdb01.database.windows.net:1433;database=BDAZE1ISQDWSV01;user=elancoadmin;password="+pswd+";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "EDNAWorkforce.Stg_Employee_Consolidated_All")
  .option("maxStrLength", "1024" )
  .option("tempDir", "wasbs://"+adls_container_name+"@"+adls_storage_account_name+".blob.core.windows.net/Curated/scratchpad")
  .mode("append")
  .save()

// COMMAND ----------

// DBTITLE 1,Load WD data to Stage History ADW(synapse)
//.conf.set("fs.azure.account.key.bdaze1iednadl01.blob.core.windows.net",key)
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


main_employee_consolidated_dataframe.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://bdaze1isqdwdb01.database.windows.net:1433;database=BDAZE1ISQDWSV01;user=elancoadmin;password="+pswd+";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "EDNAWorkforce.Stg_Employee_Consolidated_History")
  .option("maxStrLength", "1024" )
  .option("tempDir", "wasbs://"+adls_container_name+"@"+adls_storage_account_name+".blob.core.windows.net/Curated/scratchpad")
  .mode("append")
  .save()

// COMMAND ----------

// DBTITLE 1,Data cleansing
spark.sql("""SELECT NULL  AS joinkey,
inc.employee_id,
CASE 
  WHEN inc.first_name IS NULL 
        OR Upper(inc.first_name) = '''NULL''' THEN final.first_name 
  ELSE inc.first_name 
END               AS first_name, 
CASE 
  WHEN inc.middle_name IS NULL 
        OR Upper(inc.middle_name) = '''NULL''' THEN final.middle_name 
  ELSE inc.middle_name 
END               AS middle_name, 
CASE 
  WHEN inc.last_name IS NULL 
        OR Upper(inc.last_name) = '''NULL''' THEN final.last_name 
  ELSE inc.last_name 
END               AS last_name, 
CASE 
  WHEN inc.preferred_first_name IS NULL 
        OR Upper(inc.preferred_first_name) = '''NULL''' THEN 
  final.preferred_first_name 
  ELSE inc.preferred_first_name 
END               AS preferred_first_name, 
CASE 
  WHEN inc.preferred_middle_name IS NULL 
        OR Upper(inc.preferred_middle_name) = '''NULL''' THEN 
  final.preferred_middle_name 
  ELSE inc.preferred_middle_name 
END               AS preferred_middle_name, 
CASE 
  WHEN inc.preferred_last_name IS NULL 
        OR Upper(inc.preferred_last_name) = '''NULL''' THEN 
  final.preferred_last_name 
  ELSE inc.preferred_last_name 
END               AS preferred_last_name, 
CASE 
  WHEN inc.user_name IS NULL 
        OR Upper(inc.user_name) = '''NULL''' THEN final.user_name 
  ELSE inc.user_name 
END               AS user_name, 
CASE 
  WHEN inc.phone_number IS NULL 
        OR Upper(inc.phone_number) = '''NULL''' THEN final.phone_number 
  ELSE inc.phone_number 
END               AS phone_number, 
CASE 
  WHEN inc.home_phone_number IS NULL 
        OR Upper(inc.home_phone_number) = '''NULL''' THEN final.home_phone_number 
  ELSE inc.home_phone_number 
END               AS home_phone_number, 
CASE 
  WHEN inc.office_address IS NULL 
        OR Upper(inc.office_address) = '''NULL''' THEN final.office_address 
  ELSE inc.office_address 
END               AS office_address, 
CASE 
  WHEN inc.email_work IS NULL 
        OR Upper(inc.email_work) = '''NULL''' THEN final.email_work 
  ELSE inc.email_work 
END               AS email_work, 
CASE 
  WHEN inc.primary_work_email IS NULL 
        OR Upper(inc.primary_work_email) = '''NULL''' THEN final.primary_work_email 
  ELSE inc.primary_work_email 
END               AS primary_work_email, 
CASE 
  WHEN inc.location IS NULL 
        OR Upper(inc.location) = '''NULL''' THEN final.location 
  ELSE inc.location 
END               AS location, 
CASE 
  WHEN inc.location_code IS NULL 
        OR Upper(inc.location_code) = '''NULL''' THEN final.location_code 
  ELSE inc.location_code 
END               AS location_code, 
CASE 
  WHEN inc.location_address_country IS NULL 
        OR Upper(inc.location_address_country) = '''NULL''' THEN 
  final.location_address_country 
  ELSE inc.location_address_country 
END               AS location_address_country, 
CASE 
  WHEN inc.location_reference_id IS NULL 
        OR Upper(inc.location_reference_id) = '''NULL''' THEN 
  final.location_reference_id 
  ELSE inc.location_reference_id 
END               AS location_reference_id, 
CASE 
  WHEN inc.drop_code IS NULL 
        OR Upper(inc.drop_code) = '''NULL''' THEN final.drop_code 
  ELSE inc.drop_code 
END               AS drop_code, 
CASE 
  WHEN inc.affiliate IS NULL 
        OR Upper(inc.affiliate) = '''NULL''' THEN final.affiliate 
  ELSE inc.affiliate 
END               AS affiliate, 
CASE 
  WHEN inc.hub IS NULL 
        OR Upper(inc.hub) = '''NULL''' THEN final.hub 
  ELSE inc.hub 
END               AS hub, 
CASE 
  WHEN inc.position_id IS NULL 
        OR Upper(inc.position_id) = '''NULL''' THEN final.position_id 
  ELSE inc.position_id 
END               AS position_id, 
CASE 
  WHEN inc.position_title IS NULL 
        OR Upper(inc.position_title) = '''NULL''' THEN final.position_title 
  ELSE inc.position_title 
END               AS position_title, 
CASE 
  WHEN inc.previous_system_id IS NULL 
        OR Upper(inc.previous_system_id) = '''NULL''' THEN final.previous_system_id 
  ELSE inc.previous_system_id 
END               AS previous_system_id, 
CASE 
  WHEN inc.domain IS NULL 
        OR Upper(inc.domain) = '''NULL''' THEN final.domain 
  ELSE inc.domain 
END               AS domain, 
CASE 
  WHEN inc.is_manager IS NULL 
        OR Upper(inc.is_manager) = '''NULL''' THEN final.is_manager 
  ELSE inc.is_manager 
END               AS is_manager, 
CASE 
  WHEN inc.hire_date IS NULL 
        OR Upper(inc.hire_date) = '''NULL''' THEN final.hire_date 
  ELSE inc.hire_date 
END               AS hire_date, 
CASE 
  WHEN inc.original_hire_date IS NULL 
        OR Upper(inc.original_hire_date) = '''NULL''' THEN final.original_hire_date 
  ELSE inc.original_hire_date 
END               AS original_hire_date, 
CASE 
  WHEN inc.job_title IS NULL 
        OR Upper(inc.job_title) = '''NULL''' THEN final.job_title 
  ELSE inc.job_title 
END               AS job_title, 
CASE 
  WHEN inc.job_classification_id IS NULL 
        OR Upper(inc.job_classification_id) = '''NULL''' THEN 
  final.job_classification_id 
  ELSE inc.job_classification_id 
END               AS job_classification_id, 
CASE 
  WHEN inc.job_code IS NULL 
        OR Upper(inc.job_code) = '''NULL''' THEN final.job_code 
  ELSE inc.job_code 
END               AS job_code, 
CASE 
  WHEN inc.job_family IS NULL 
        OR Upper(inc.job_family) = '''NULL''' THEN final.job_family 
  ELSE inc.job_family 
END               AS job_family, 
CASE 
  WHEN inc.job_family_group IS NULL 
        OR Upper(inc.job_family_group) = '''NULL''' THEN final.job_family_group 
  ELSE inc.job_family_group 
END               AS job_family_group, 
CASE 
  WHEN inc.active_status IS NULL 
        OR Upper(inc.active_status) = '''NULL''' THEN final.active_status 
  ELSE inc.active_status 
END               AS active_status, 
CASE 
  WHEN inc.contract_end_date IS NULL 
        OR Upper(inc.contract_end_date) = '''NULL''' THEN final.contract_end_date 
  ELSE inc.contract_end_date 
END               AS contract_end_date, 
CASE 
  WHEN inc.supplier IS NULL 
        OR Upper(inc.supplier) = '''NULL''' THEN final.supplier 
  ELSE inc.supplier 
END               AS supplier, 
CASE 
  WHEN inc.secondary_vendor IS NULL 
        OR Upper(inc.secondary_vendor) = '''NULL''' THEN final.secondary_vendor 
  ELSE inc.secondary_vendor 
END               AS secondary_vendor, 
CASE 
  WHEN inc.manager_employee_id IS NULL 
        OR Upper(inc.manager_employee_id) = '''NULL''' THEN 
  final.manager_employee_id 
  ELSE inc.manager_employee_id 
END               AS manager_employee_id, 
CASE 
  WHEN inc.supervisory_org_name IS NULL 
        OR Upper(inc.supervisory_org_name) = '''NULL''' THEN 
  final.supervisory_org_name 
  ELSE inc.supervisory_org_name 
END               AS supervisory_org_name, 
CASE 
  WHEN inc.supervisory_organization_id IS NULL 
        OR Upper(inc.supervisory_organization_id) = '''NULL''' THEN 
  final.supervisory_organization_id 
  ELSE inc.supervisory_organization_id 
END               AS supervisory_organization_id, 
CASE 
  WHEN inc.time_type IS NULL 
        OR Upper(inc.time_type) = '''NULL''' THEN final.time_type 
  ELSE inc.time_type 
END               AS time_type, 
CASE 
  WHEN inc.worker_type IS NULL 
        OR Upper(inc.worker_type) = '''NULL''' THEN final.worker_type 
  ELSE inc.worker_type 
END               AS worker_type, 
CASE 
  WHEN inc.cost_center_id IS NULL 
        OR Upper(inc.cost_center_id) = '''NULL''' THEN final.cost_center_id 
  ELSE inc.cost_center_id 
END               AS cost_center_id, 
CASE 
  WHEN inc.cost_centre IS NULL 
        OR Upper(inc.cost_centre) = '''NULL''' THEN final.cost_centre 
  ELSE inc.cost_centre 
END               AS cost_centre, 
CASE 
  WHEN inc.cost_center_name IS NULL 
        OR Upper(inc.cost_center_name) = '''NULL''' THEN final.cost_center_name 
  ELSE inc.cost_center_name 
END               AS cost_center_name, 
CASE 
  WHEN inc.worker_sub_type IS NULL 
        OR Upper(inc.worker_sub_type) = '''NULL''' THEN final.worker_sub_type 
  ELSE inc.worker_sub_type 
END               AS worker_sub_type, 
CASE 
  WHEN inc.contingent_worker_supplier IS NULL 
        OR Upper(inc.contingent_worker_supplier) = '''NULL''' THEN 
  final.contingent_worker_supplier 
  ELSE inc.contingent_worker_supplier 
END               AS contingent_worker_supplier, 
CASE 
  WHEN inc.currently_active IS NULL 
        OR Upper(inc.currently_active) = '''NULL''' THEN final.currently_active 
  ELSE inc.currently_active 
END               AS currently_active, 
CASE 
  WHEN inc.retiree IS NULL 
        OR Upper(inc.retiree) = '''NULL''' THEN final.retiree 
  ELSE inc.retiree 
END               AS retiree, 
CASE 
  WHEN inc.rsa_unique_personel_id IS NULL 
        OR Upper(inc.rsa_unique_personel_id) = '''NULL''' THEN 
  final.rsa_unique_personel_id 
  ELSE inc.rsa_unique_personel_id 
END               AS rsa_unique_personel_id, 
CASE 
  WHEN inc.rsa_email_work IS NULL 
        OR Upper(inc.rsa_email_work) = '''NULL''' THEN final.rsa_email_work 
  ELSE inc.rsa_email_work 
END               AS rsa_email_work, 
inc.active_ind, 
inc.load_datetime,
inc.datepart 
FROM   workforce_stage.stg_employee_consolidated_inc inc 
       JOIN workforce_curated.employee_consolidated final 
       ON inc.employee_id = final.employee_id 
       WHERE  final.active_ind <> '''N'''
""").createOrReplaceTempView("modifiedrecordsofexistingemployees")

// COMMAND ----------

// DBTITLE 1,Load Data to final table
//select null as joinkey,inc.* from workforce_stage.stg_employee_consolidated_inc inc join workforce_curated.employee_consolidated_final final on inc.employee_id=final.employee_id where final.active_ind<>'''N'''
spark.sql("""MERGE INTO workforce_curated.employee_consolidated ec
USING( select inc.employee_id as joinkey,inc.* from workforce_stage.stg_employee_consolidated_inc inc 
union all
select * from modifiedrecordsofexistingemployees
)newrecords
ON ec.Employee_ID=newrecords.joinkey
WHEN MATCHED and ec.active_ind <>'''N''' THEN
  UPDATE SET ec.active_ind ='''N''',
  ec.Effective_Enddate=ifnull(date_add(newrecords.Load_Datetime,-1),current_timestamp())
WHEN NOT MATCHED
  THEN INSERT (
  datepart
,employee_id
,first_name
,middle_name
,last_name
,preferred_first_name
,preferred_middle_name
,preferred_last_name
,user_name
,phone_number
,home_phone_number
,office_address
,email_work
,primary_work_email
,location
,location_code
,location_address_country
,location_reference_id
,drop_code
,affiliate
,hub
,position_id
,position_title
,previous_system_id
,domain
,is_manager
,hire_date
,original_hire_date
,job_title
,job_classification_id
,job_code
,job_family
,job_family_group
,active_status
,contract_end_date
,supplier
,secondary_vendor
,manager_employee_id
,supervisory_org_name
,supervisory_organization_id
,time_type
,worker_type
,cost_center_id
,cost_centre
,cost_center_name
,worker_sub_type
,contingent_worker_supplier
,currently_active
,retiree
,rsa_unique_personel_id
,rsa_email_work
,active_ind
,Effective_Startdate
,Effective_Enddate
,load_datetime
) VALUES (
newrecords.datepart
,newrecords.employee_id
,newrecords.first_name
,newrecords.middle_name
,newrecords.last_name
,newrecords.preferred_first_name
,newrecords.preferred_middle_name
,newrecords.preferred_last_name
,newrecords.user_name
,newrecords.phone_number
,newrecords.home_phone_number
,newrecords.office_address
,newrecords.email_work
,newrecords.primary_work_email
,newrecords.location
,newrecords.location_code
,newrecords.location_address_country
,newrecords.location_reference_id
,newrecords.drop_code
,newrecords.affiliate
,newrecords.hub
,newrecords.position_id
,newrecords.position_title
,newrecords.previous_system_id
,newrecords.domain
,newrecords.is_manager
,newrecords.hire_date
,newrecords.original_hire_date
,newrecords.job_title
,newrecords.job_classification_id
,newrecords.job_code
,newrecords.job_family
,newrecords.job_family_group
,newrecords.active_status
,newrecords.contract_end_date
,newrecords.supplier
,newrecords.secondary_vendor
,newrecords.manager_employee_id
,newrecords.supervisory_org_name
,newrecords.supervisory_organization_id
,newrecords.time_type
,newrecords.worker_type
,newrecords.cost_center_id
,newrecords.cost_centre
,newrecords.cost_center_name
,newrecords.worker_sub_type
,newrecords.contingent_worker_supplier
,newrecords.currently_active
,newrecords.retiree
,newrecords.rsa_unique_personel_id
,newrecords.rsa_email_work
,newrecords.active_ind
,ifnull(newrecords.Load_Datetime,current_timestamp())
,to_timestamp("9999-12-31 00:00:00.000", "yyyy-MM-dd HH:mm:ss")
,newrecords.load_datetime
)""")


// COMMAND ----------

val test_employee_consolidated_dataframe=spark.sql("select  * from workforce_curated.employee_consolidated")

// COMMAND ----------

val employee_consolidated_dataframe_inc = test_employee_consolidated_dataframe.drop($"datepart")

// COMMAND ----------

// DBTITLE 1,Load WD data to Final ADW(synapse)
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


employee_consolidated_dataframe_inc.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://bdaze1isqdwdb01.database.windows.net:1433;database=BDAZE1ISQDWSV01;user=elancoadmin;password="+pswd+";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "EDNAWorkforce.Employee_Consolidated_Temp")
  .option("tempDir", "wasbs://"+adls_container_name+"@"+adls_storage_account_name+".blob.core.windows.net/Curated/scratchpad")
  .mode("overwrite")
  .save()