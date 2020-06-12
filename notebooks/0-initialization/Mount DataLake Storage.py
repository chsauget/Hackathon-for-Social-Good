# Databricks notebook source
# MAGIC %md
# MAGIC # Mount DataLake Storage
# MAGIC This notebook is used to mount an Azure Data Lake Storage Gen2 filesystem to DBFS using a service principal.
# MAGIC Service principal credentials (AppId, AppSecretKey and TenantId) are stored in an Azure Key Vault-backed secret scope.
# MAGIC 
# MAGIC ## Usage
# MAGIC There are two filesystems to mount to DBFS : 
# MAGIC - **datasets** : contains the raw and parquet data
# MAGIC - **datawarehouse** : contains the Delta Lake files for the self-service usage
# MAGIC 
# MAGIC ## References 
# MAGIC - [Azure Databricks - Azure Data Lake Storage Gen2](https://docs.databricks.com/data/data-sources/azure/azure-datalake-gen2.html)
# MAGIC - [Azure Key Vault-backed scopes](https://docs.microsoft.com/fr-fr/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes)

# COMMAND ----------

# DBTITLE 1,Parameters
KEYVAULT_NAME = "kvhackthon"

STORAGE_NAME = dbutils.secrets.get(scope = KEYVAULT_NAME, key = "dbk-storagename")

APPLICATION_ID = dbutils.secrets.get(scope = KEYVAULT_NAME, key = "dbk-applicationid")
APPLICATION_KEY = dbutils.secrets.get(scope = KEYVAULT_NAME, key = "dbk-applicationkey")
DIRECTORY_ID = dbutils.secrets.get(scope = KEYVAULT_NAME, key = "dbk-directoryid")

# COMMAND ----------

# DBTITLE 1,Mount filesytems on cluster
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": APPLICATION_ID,
  "fs.azure.account.oauth2.client.secret": APPLICATION_KEY,
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+DIRECTORY_ID+"/oauth2/token"
} 

dbutils.fs.mount(
  source = "abfss://datasets@"+STORAGE_NAME+".dfs.core.windows.net/",
  mount_point = "/mnt/datasets",
  extra_configs = configs) 

dbutils.fs.mount(
  source = "abfss://datawarehouse@"+STORAGE_NAME+".dfs.core.windows.net/",
  mount_point = "/mnt/datawarehouse",
  extra_configs = configs) 

# COMMAND ----------

# DBTITLE 1,Test filesystems
dbutils.fs.ls("/mnt/datasets")
dbutils.fs.ls("/mnt/datawarehouse")