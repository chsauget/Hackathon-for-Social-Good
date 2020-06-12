# Databricks notebook source
# MAGIC %md
# MAGIC #Orchestrate Data Extraction
# MAGIC This notebook is used to orchestrate and parallelize the extraction of the datasets from the data source Climate Data Store (Copernicus).
# MAGIC 
# MAGIC Unitary extraction notebook : *Retrieve Data From CDS*

# COMMAND ----------

#satellite-carbon-dioxide extraction

dbutils.notebook.run("Retrieve Data From CDS"
                     , 86400
                     , { "datasetName" : "satellite-carbon-dioxide" 
                          , "datasetStructure" : """{
        'format': 'zip',
        'sensor_and_algorithm': 'merged_emma',
        'year': [
             '2003', '2004', '2005',
            '2006', '2007', '2008',
            '2009', '2010', '2011',
            '2012', '2013', '2014',
            '2015', '2016', '2017',
            '2018'
        ],
        'month': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
        ],
        'day': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ],
        'version': '4.1',
        'variable': 'xco2',
        'processing_level': 'level_2',
    }"""
                         ,"folderPath": "/dbfs/mnt/datasets/satellite-carbon-dioxide/"
                       }
                    )

# COMMAND ----------

#convert nc2parquet
dbutils.notebook.run("Convert NetCDF-GRIB to parquet"
                 , 86400
                 ,{
                   "folderPath": "/dbfs/mnt/datasets/satellite-carbon-dioxide/data/"
                   , "dfDrop": "['time','xco2_quality_flag','contributing_algorithms','median_processor_id']"
                   , "dfTransform": """{
                                        "datetime": "pd.to_datetime(df.time, unit='s').dt.date",
                                        "year": "pd.DatetimeIndex(df['datetime']).year",
                                        "month": "pd.DatetimeIndex(df['datetime']).month"
                                      }"""
                   , "parquetPath": "/dbfs/mnt/datasets/satellite-carbon-dioxide/parquet/"
                   , "partitionByColumns": "['year','month']"
                   , "threadPool" : "10"
                  }
                )


# COMMAND ----------

#ecv-for-climate-change - Essential climate variables for assessment of climate variability from 1979 to present
#Surface air temperature
#Surface air relative humidity
#0-7cm volumetric soil moisture
#Precipitation
#Sea-ice cover
import datetime

for y in range(1979,datetime.datetime.now().year):
  for variable in ['0_7cm_volumetric_soil_moisture', 'precipitation', 'sea_ice_cover','surface_air_relative_humidity', 'surface_air_temperature']:
    folderPath = "/dbfs/mnt/datasets/ecv-for-climate-change-%s"%variable
    dbutils.notebook.run("Retrieve Data From CDS"
                       , 86400
                       , { "datasetName" : "ecv-for-climate-change"
                            , "datasetStructure" : """{
                                                        'format': 'zip',
                                                        'variable': ['%s'],
                                                        'product_type': [
                                                            'anomaly', 'climatology', 'monthly_mean',
                                                        ],
                                                        'time_aggregation': '1_month',
                                                        'year': '%s',
                                                        'month': [
                                                            '01', '02', '03',
                                                            '04', '05', '06',
                                                            '07', '08', '09',
                                                            '10', '11', '12',
                                                        ],
                                                        'origin': [
                                                            'era5','era_interim'
                                                        ],
                                                    }"""%(variable,y)
                          ,"folderPath": folderPath
                         }
                      )
    #convert nc2parquet
    dbutils.notebook.run("Convert NetCDF-GRIB to parquet"
               , 86400
               ,{
                 "folderPath": "%s/data/"%folderPath
                 , "dfDrop": "['step']"
                 , "dfTransform": """{
                            "datetime": "pd.to_datetime(df.time, unit='s')",
                            "year": "pd.DatetimeIndex(df['datetime']).year",
                            "month": "pd.DatetimeIndex(df['datetime']).month",
                            "filename": "f"
                          }"""
                 , "parquetPath": "%s/parquet/"%folderPath
                 , "partitionByColumns": "['year','month']"
                 , "threadPool" : "50"
                }
              )
    