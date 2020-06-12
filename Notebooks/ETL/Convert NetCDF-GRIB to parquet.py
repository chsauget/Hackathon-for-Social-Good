# Databricks notebook source
# MAGIC %md
# MAGIC #Convert NetCDF-GRIB to parquet
# MAGIC 
# MAGIC This notebook is used to convert all files of a specified folder from GRIB or NetCDF format to Parquet
# MAGIC 
# MAGIC ### Parameters
# MAGIC - folderPath : path of the folder containing the files to convert in parquet
# MAGIC - fileName : path of the file to convert in parquet
# MAGIC - dfTransform : Pandas dataframe transformation to apply during conversion
# MAGIC - dfDrop : Dataframe columns to drop during conversion
# MAGIC - partitionByColumns : columns used for partitionning the parquet file
# MAGIC - parquetPath : path of the output folder containing the parquet files
# MAGIC - threadPool : number of threads for parallel conversion

# COMMAND ----------

# DBTITLE 1,Notebook parameters
#NC File Path
dbutils.widgets.text("folderPath", "/dbfs/mnt/datasets/satellite-carbon-dioxide/data/")
folderPath = dbutils.widgets.get("folderPath")
#NC File Name
dbutils.widgets.text("fileName", "20060227-C3S-L2_GHG-GHG_PRODUCTS-MERGED-MERGED-EMMA-DAILY-v4.1.nc")
fileName = dbutils.widgets.get("fileName")

#pandas dataframe transformation
dbutils.widgets.text("dfTransform", """{
                                        "datetime": "pd.to_datetime(df.time, unit='s')",
                                        "year": "pd.DatetimeIndex(df['datetime']).year",
                                        "month": "pd.DatetimeIndex(df['datetime']).month"
                                      }""")
dfTransform = eval(dbutils.widgets.get("dfTransform"))
#pandas dataframe drop columns
dbutils.widgets.text("dfDrop", "['time','co2_quality_flag']")
dfDrop = eval(dbutils.widgets.get("dfDrop"))
#output partition columns
dbutils.widgets.text("partitionByColumns", "['year','month']")
partitionByColumns = eval(dbutils.widgets.get("partitionByColumns"))
#output path
dbutils.widgets.text("parquetPath", "/dbfs/mnt/datasets/satellite-carbon-dioxide/parquet2/")
parquetPath = dbutils.widgets.get("parquetPath")
#nb of threadpool
dbutils.widgets.text("threadPool", "20")
threadPool = dbutils.widgets.get("threadPool")

# COMMAND ----------

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pathlib

# COMMAND ----------

# DBTITLE 1,Execute parallel conversion
import itertools
from multiprocessing.pool import ThreadPool

if threadPool.strip():
  #Chunk the files to avoid notebook suralocation
  files = [str(x.name) for x in pathlib.Path(folderPath).glob('*')]  
  def grouper(n, iterable, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)

  grouped_files = list(grouper(20, files))
  listOfjoinedFiles = []
  for grouped_file in grouped_files:
    joinedFileName = ','.join(str(item) for item in filter(None, grouped_file))
    listOfjoinedFiles.append(joinedFileName)
    
  pool = ThreadPool(int(threadPool))
    
  pool.map(lambda file: dbutils.notebook.run("Convert NetCDF-GRIB to parquet"
                 , 86400
                 ,{
                   "folderPath": folderPath
                   ,"fileName": str(file)
                   , "dfDrop": str(dfDrop)
                   , "dfTransform": str(dfTransform)
                   , "parquetPath": str(parquetPath)
                   , "partitionByColumns": str(partitionByColumns)
                   , "threadPool": ""
                  }
                )
         ,listOfjoinedFiles
        )

# COMMAND ----------

# DBTITLE 1,Conversion to parquet
if not threadPool.strip():
  import pyarrow as pa
  import pandas as pd
  import pyarrow.parquet as pq
  import xarray as xr

  try:
    
    fileNameList = fileName.split(',')
    if fileNameList[0].endswith('.nc'):
      engine="netcdf4"
    if fileNameList[0].endswith('.grib'):
      engine="cfgrib"
    
    for f in fileNameList:
      try:
        ds = xr.open_dataset('%s/%s'%(folderPath,f), engine=engine, cache=False)
        print('** %s/%s converted **'%(folderPath,f))
        df = ds.to_dataframe()
        i=0
        for d in ds.dims:
          df[d] = df.index.get_level_values(i)
          i=i+1

        df = df.reset_index(drop=True)

        #Add new columns based on parameters
        for col in dfTransform:
          df[col] = eval(dfTransform[col])

        #Drop columns
        df = df.drop(dfDrop, axis=1)  
         
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table=table, root_path=parquetPath,partition_cols=partitionByColumns)

        print('** %s/%s converted **'%(folderPath,f))
      except Exception as e:
        print(e)
  except Exception as e:
    print(e)