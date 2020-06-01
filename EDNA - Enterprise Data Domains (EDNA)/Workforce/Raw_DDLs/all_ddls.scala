// Databricks notebook source
// DBTITLE 1,Declare Variables
val secret=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-ClintID")
val tenantid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-TenantID")
val adls_storage_account_name = "bdaze1iednadl01"
val adls_container_name = "edna-workforcedatadomain" 
val emp_consolidatedpath = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Curated/employee_consolidated_final"
val emp_raw = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Raw/"
val emp_stage = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Stage/"
val emp_curated = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Curated/"
//val emp_consolidatedpath = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/"+"Curated/employee_consolidated_final"//edit
//abfss://edna-workforcedatadomain@bdaze1iednadl01.dfs.core.windows.net/Curated/

// COMMAND ----------

// DBTITLE 1,Configure source(ADLS) - spark conf
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

// DBTITLE 1,Configure source(ADLS) - hadoop conf
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type."+adls_storage_account_name+"", "OAuth")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

// DBTITLE 1,workforce_raw- rsa
spark.sql("drop table if exists workforce_raw.rsa_employee")//cmd9

// COMMAND ----------

spark.sql("CREATE TABLE workforce_raw.rsa_employee(id integer,user_id string,idc_id string,first_name string,middlename string,last_name string,preferred_firstname string,preferred_middlename string,preferred_lastname string,phone string,email_address string,lilyemail string,location_ string,address string,department string,title string,business_unit_id string,contingent_workertype string,jml_status string,job_status string,availability_status string,hire_date string,termination_date string,is_terminated string,creation_date string,deletion_date string,samaccount_name string,supervisor_id string,supervisor_id_name string,backup_supervisor string,backup_supervisor_name string,exception_count string,unique_id string,upn string,add_state string,remove_state string,notused string,notused1 string,violation_count string,worker_type string,load_datetime timestamp,file_name string,datepart date) USING delta PARTITIONED BY(datepart) LOCATION '"+emp_raw+"RSA/employee_rsa_raw'")//cmd10

// COMMAND ----------

// DBTITLE 1,workforce_stage-rsa
spark.sql("drop table if exists workforce_stage.rsa_employee_stg")

// COMMAND ----------

//val lakePath = "abfss://edna-workforcedatadomain@bdaze1iednadl01.dfs.core.windows.net/Raw/"
spark.sql("CREATE TABLE workforce_stage.rsa_employee_stg(id integer,user_id string,idc_id string,first_name string,middlename string,last_name string,preferred_firstname string,preferred_middlename string,preferred_lastname string,phone string,email_address string,lilyemail string,location_ string,address string,department string,title string,business_unit_id string,contingent_workertype string,jml_status string,job_status string,availability_status string,hire_date string,termination_date string,is_terminated string,creation_date string,deletion_date string,samaccount_name string,supervisor_id string,supervisor_id_name string,backup_supervisor string,backup_supervisor_name string,exception_count string,unique_id string,upn string,add_state string,remove_state string,notused string,notused1 string,violation_count string,worker_type string,load_datetime timestamp,file_name string,datepart date) USING delta PARTITIONED BY(datepart) LOCATION '"+emp_stage+"RSA/employee_rsa'");

// COMMAND ----------

spark.sql("drop table if exists workforce_stage.rsa_employee_inc")

// COMMAND ----------

spark.sql("CREATE TABLE workforce_stage.rsa_employee_inc(id integer,user_id string,idc_id string,first_name string,middlename string,last_name string,preferred_firstname string,preferred_middlename string,preferred_lastname string,phone string,email_address string,lilyemail string,location_ string,address string,department string,title string,business_unit_id string,contingent_workertype string,jml_status string,job_status string,availability_status string,hire_date string,termination_date string,is_terminated string,creation_date string,deletion_date string,samaccount_name string,supervisor_id string,supervisor_id_name string,backup_supervisor string,backup_supervisor_name string,exception_count string,unique_id string,upn string,add_state string,remove_state string,notused string,notused1 string,violation_count string,worker_type string,load_datetime timestamp,file_name string,datepart date) USING delta PARTITIONED BY(datepart) LOCATION '"+emp_stage+"RSA/employee_rsa_inc'");

// COMMAND ----------

// DBTITLE 1,workforce_raw- workday
spark.sql("drop table if exists workforce_raw.wd_employee")

// COMMAND ----------

spark.sql("CREATE TABLE workforce_raw.wd_employee(employee_id integer,user_name string,first_name string,middle_name string,last_name string,preferred_name_first_name string,preferred_name_middle_name string,preferred_name_last_name string,phone_number string,home_phone_number string,primary_work_email string,location string,location_address_country string,location_reference_id string,position_id integer,position_title string,previous_system_id string,is_manager string,hire_date timestamp,original_hire_date timestamp,job_classification_id string,job_code string,job_family string,job_family_group string,active_status string,contract_end_date timestamp,manager_employee_id integer,supervisory_org_name string,supervisory_organization_id string,cost_center_id string,cost_centre string,cost_center_name string,contingent_worker_supplier string,time_type string,worker_type string,worker_sub_type string,load_datetime timestamp,file_name string,datepart date) USING delta PARTITIONED BY(datepart) LOCATION '"+emp_raw+"/Workday/employee_wd_raw'");

// COMMAND ----------

// DBTITLE 1,workforce_stage - workday
spark.sql("drop table if exists workforce_stage.wd_employee_stg")

// COMMAND ----------

spark.sql("CREATE TABLE workforce_stage.wd_employee_stg(employee_id integer,user_name string,first_name string,middle_name string,last_name string,preferred_name_first_name string,preferred_name_middle_name string,preferred_name_last_name string,phone_number string,home_phone_number string,primary_work_email string,location string,location_address_country string,location_reference_id string,position_id integer,position_title string,previous_system_id string,is_manager string,hire_date timestamp,original_hire_date timestamp,job_classification_id string,job_code string,job_family string,job_family_group string,active_status string,contract_end_date timestamp,manager_employee_id integer,supervisory_org_name string,supervisory_organization_id string,cost_center_id string,cost_centre string,cost_center_name string,contingent_worker_supplier string,time_type string,worker_type string,worker_sub_type string,load_datetime timestamp,file_name string,datepart date) USING delta PARTITIONED BY(datepart) LOCATION '"+emp_stage+"/Workday/employee_workday'");

// COMMAND ----------

spark.sql("drop table if exists workforce_stage.wd_employee_inc")

// COMMAND ----------

/*spark.sql("CREATE TABLE workforce_stage.wd_employee_inc(datepart string,employee_id integer,user_name string,first_name string,middle_name string,last_name string,preferred_name_first_name string,preferred_name_middle_name string,preferred_name_last_name string,phone_number string,home_phone_number string,primary_work_email string,location string,location_address_country string,location_reference_id string,position_id integer,position_title string,previous_system_id string,is_manager string,hire_date timestamp,original_hire_date timestamp,job_classification_id string,job_code string,job_family string,job_family_group string,active_status string,contract_end_date timestamp,manager_employee_id integer,supervisory_org_name string,supervisory_organization_id string,cost_center_id string,cost_centre string,cost_center_name string,contingent_worker_supplier string,time_type string,worker_type string,worker_sub_type string,load_datetime timestamp,file_name string)  USING delta PARTITIONED BY(datepart)  LOCATION '"+emp_stage+"/Workday/employee_wd_inc'");
*/

spark.sql("CREATE TABLE workforce_stage.wd_employee_inc(employee_id integer,user_name string,first_name string,middle_name string,last_name string,preferred_name_first_name string,preferred_name_middle_name string,preferred_name_last_name string,phone_number string,home_phone_number string,primary_work_email string,location string,location_address_country string,location_reference_id string,position_id integer,position_title string,previous_system_id string,is_manager string,hire_date timestamp,original_hire_date timestamp,job_classification_id string,job_code string,job_family string,job_family_group string,active_status string,contract_end_date timestamp,manager_employee_id integer,supervisory_org_name string,supervisory_organization_id string,cost_center_id string,cost_centre string,cost_center_name string,contingent_worker_supplier string,time_type string,worker_type string,worker_sub_type string,load_datetime timestamp,file_name string,datepart date)  USING delta PARTITIONED BY(datepart)  LOCATION '"+emp_stage+"/Workday/employee_wd_inc'");

// COMMAND ----------

// DBTITLE 1,workforce_stage-consolidated
spark.sql("drop table if exists workforce_stage.stg_employee_consolidated")

// COMMAND ----------

spark.sql("CREATE TABLE workforce_stage.stg_employee_consolidated(employee_id integer,first_name string,middle_name string,last_name string,preferred_first_name string,preferred_middle_name string,preferred_last_name string,user_name string,phone_number string,home_phone_number string,office_address string,email_work string,primary_work_email string,location string,location_code string,location_address_country string,location_reference_id string,drop_code string,affiliate string,hub string,position_id integer,position_title string,previous_system_id string,domain string,is_manager string,hire_date timestamp,original_hire_date timestamp,job_title string,job_classification_id string,job_code string,job_family string,job_family_group string,active_status string,contract_end_date timestamp,supplier string,secondary_vendor string,manager_employee_id integer,supervisory_org_name string,supervisory_organization_id string,time_type string,worker_type string,cost_center_id string,cost_centre string,cost_center_name string,worker_sub_type string,contingent_worker_supplier string,currently_active string,retiree string,rsa_unique_personel_id string,rsa_email_work string,active_ind string,load_datetime timestamp,datepart date)  USING delta PARTITIONED BY(datepart) LOCATION '"+emp_stage+"/stg_employee_consolidated'");

// COMMAND ----------

spark.sql("drop table if exists workforce_stage.stg_employee_consolidated_inc")

// COMMAND ----------

spark.sql("CREATE TABLE workforce_stage.stg_employee_consolidated_inc(employee_id integer,first_name string,middle_name string,last_name string,preferred_first_name string,preferred_middle_name string,preferred_last_name string,user_name string,phone_number string,home_phone_number string,office_address string,email_work string,primary_work_email string,location string,location_code string,location_address_country string,location_reference_id string,drop_code string,affiliate string,hub string,position_id integer,position_title string,previous_system_id string,domain string,is_manager string,hire_date timestamp,original_hire_date timestamp,job_title string,job_classification_id string,job_code string,job_family string,job_family_group string,active_status string,contract_end_date timestamp,supplier string,secondary_vendor string,manager_employee_id integer,supervisory_org_name string,supervisory_organization_id string,time_type string,worker_type string,cost_center_id string,cost_centre string,cost_center_name string,worker_sub_type string,contingent_worker_supplier string,currently_active string,retiree string,rsa_unique_personel_id string,rsa_email_work string,active_ind string,load_datetime timestamp, datepart date)  USING delta PARTITIONED BY(datepart)  LOCATION '"+emp_stage+"/stg_employee_consolidated_inc'");

// COMMAND ----------

// DBTITLE 1,workforce_curated
spark.sql("drop table if exists workforce_curated.employee_consolidated")

// COMMAND ----------

val lakePath = "abfss://edna-workforcedatadomain@bdaze1iednadl01.dfs.core.windows.net/Raw/"
spark.sql("CREATE TABLE workforce_curated.employee_consolidated(employee_id integer,first_name string,middle_name string,last_name string,preferred_first_name string,preferred_middle_name string,preferred_last_name string,user_name string,phone_number string,home_phone_number string,office_address string,email_work string,primary_work_email string,location string,location_code string,location_address_country string,location_reference_id string,drop_code string,affiliate string,hub string,position_id integer,position_title string,previous_system_id string,domain string,is_manager string,hire_date timestamp,original_hire_date timestamp,job_title string,job_classification_id string,job_code string,job_family string,job_family_group string,active_status string,contract_end_date timestamp,supplier string,secondary_vendor string,manager_employee_id integer,supervisory_org_name string,supervisory_organization_id string,time_type string,worker_type string,cost_center_id string,cost_centre string,cost_center_name string,worker_sub_type string,contingent_worker_supplier string,currently_active string,retiree string,rsa_unique_personel_id string,rsa_email_work string,active_ind string,Effective_Startdate timestamp,Effective_Enddate timestamp,load_datetime timestamp,datepart date ) USING delta PARTITIONED BY(datepart) LOCATION '"+emp_consolidatedpath+"'");