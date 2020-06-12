# Databricks notebook source
# MAGIC %md
# MAGIC #Retrieve Data From CDS
# MAGIC This notebook is used to extract a dataset (in zipped GRIB or NetCDF format) from the data source Copernicus Climate Data Store using a REST API.
# MAGIC API url and key are stored in an Azure Key Vault-backed secret scope.
# MAGIC 
# MAGIC The dataset files are stored in an Azure Data Lake Storage filesystem (one folder for each dataset).
# MAGIC 
# MAGIC ## References 
# MAGIC - [How to use the CDS API](https://cds.climate.copernicus.eu/api-how-to)
# MAGIC - [Azure Key Vault-backed scopes](https://docs.microsoft.com/fr-fr/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes)

# COMMAND ----------

#Parameters
KEYVAULT_NAME = "kvhackthon"
#dataset name
dbutils.widgets.text("datasetName", "satellite-carbon-dioxide")
datasetName = dbutils.widgets.get("datasetName")
#folder path
dbutils.widgets.text("folderPath", "/dbfs/mnt/datasets/satellite-carbon-dioxide/")
folderPath = dbutils.widgets.get("folderPath")
#dataset structure
dbutils.widgets.text("datasetStructure", """{ 
        'processing_level': 'level_2', 
        'variable': 'co2',
        'sensor_and_algorithm': 'iasi_metop_b_nlis',
        'year': '2018',
        'month': '06',
        'day': '06',
        'version': '4.2',
        'format': 'zip',
    }""")
datasetStructure = dbutils.widgets.get("datasetStructure")

zipDatasetPath = '{0}/zip'.format(folderPath)
tempDatasetPath = '{0}/tempData'.format(folderPath)
datasetPath = '{0}/data'.format(folderPath)

# COMMAND ----------

#Create folder for datas
dbutils.fs.mkdirs('file:{0}'.format(zipDatasetPath))
dbutils.fs.mkdirs('file:{0}'.format(datasetPath))

# COMMAND ----------

import cdsapi
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

c = cdsapi.Client(url=dbutils.secrets.get(scope = KEYVAULT_NAME, key = "cdsapi-url"),
                  key=dbutils.secrets.get(scope = KEYVAULT_NAME, key = "cdsapi-user-key"),
                 quiet=True)

c.retrieve(
    datasetName,
    eval(datasetStructure),
    '{0}/{1}.zip'.format(zipDatasetPath,datasetName)
)

# COMMAND ----------

import zipfile
with zipfile.ZipFile('{0}/{1}.zip'.format(zipDatasetPath,datasetName), 'r') as zip_ref:
    zip_ref.extractall(datasetPath)