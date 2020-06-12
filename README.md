# Hackathon for Social Good
Repository for the Databricks Hackathon for Social Good !

## Installation

- Create an Azure Databricks workspace
- Create support ressources : Azure Storage, Azure Key Vault
- Create the Azure Key-Vault backed secret scope in the Databricks workspace [[documentation](https://docs.microsoft.com/fr-fr/azure/databricks/security/secrets/secret-scopes#create-an-azure-key-vault-backed-secret-scope)]
- Create a Databricks cluster
- Run [initialization notebooks](/notebooks/initialization)


## Required librairies
| Librairy name                                          | Source | Stable version |
|--------------------------------------------------------|--------|----------------|
| [cdsapi](https://pypi.org/project/cdsapi/)             | PyPI   | 0.2.7          |
| [cfgrib](https://pypi.org/project/cfgrib/)             | PyPI   | 0.9.8.1        |
| [eccodes](https://pypi.org/project/eccodes/)           | PyPI   | 0.9.7          |
| [geopandas](https://pypi.org/project/geopandas/)       | PyPI   | 0.7.0          |
| [more-itertools](https://pypi.org/project/more-itertools/) | PyPI   | 8.3.0      |
| [netCDF4](https://pypi.org/project/netCDF4/)           | PyPI   | 1.5.3          |
| [pycountry](https://pypi.org/project/pycountry/)       | PyPI   | 19.8.18        |
| [pyeccodes](https://pypi.org/project/pyeccodes/)       | PyPI   | 0.1.1          |
| [pygeohash](https://pypi.org/project/pygeohash/)       | PyPI   | 1.2.0          |
| [reverse_geocode](https://pypi.org/project/reverse_geocode/) | PyPI   | 1.5.1    |
| [xarray](https://pypi.org/project/xarray/)             | PyPI   | 0.15.1         |

## Data preparation

### Loading data from CDS

The data are retrieve from [CDS (Climate Data Store)](https://cds.climate.copernicus.eu/cdsapp#!/home) through their API with the help of the CDS API Client in python.

Because of the large number of dataset we planned to use we dedicated [a parametrized notebook](notebooks/1-data-preparation/Retrieve%20Data%20From%20CDS.py) to the task of retrieving the data from CDS. This way we standardized the download and extract from zip steps and the folder organization.

```python
dbutils.notebook.run("Retrieve Data From CDS"
                     , 86400
                     , { "datasetName" : "satellite-carbon-dioxide" 
                          , "datasetStructure" : """{
                                'format': 'zip',
                                'sensor_and_algorithm': 'merged_emma',
                                'year': [
                                '2003', '2004', '2005'
                                ],
                                'month': [
                                '01', '02'
                                ],
                                'day': [
                                '01'
                                ],
                                'version': '4.1',
                                'variable': 'xco2',
                                'processing_level': 'level_2',
                                }"""
                        ,"folderPath": "/dbfs/mnt/datasets/satellite-carbon-dioxide/"
                }
        )
```

This way we are also able to avoid some limitation of the API and when needed iterate over month, or axes easily.

### Converting files to Parquet

The retrieved files from Copernicus are NetCDF and GRIB files, as their are multidimensionnal files and not directly supported by spark, we choose to convert them to parquet.

To keep the maintenance effort and the code as atomic a possible we created a [dedicated parametrized notebook](notebooks/1-data-preparation/Convert%20NetCDF-GRIB%20to%20parquet.py). It will also allow us to parallize the conversion on multiple notebook execution using ThreadPool. Sadly the conversion will be moexecuted on the head node that's why we dedicated a specificaly [configurated cluster](clusters/bigdriver_cluster.json) to this task.

The notebook should be configured with the following parameters : 

- **folderPath :** path of the folder containing the files to convert in parquet
- **fileName :** path of the file to convert in parquet
- **dfTransform :** Pandas dataframe transformation to apply during conversion
- **dfDrop :** Dataframe columns to drop during conversion
- **partitionByColumns :** columns used for partitionning the parquet file
- **parquetPath :** path of the output folder containing the parquet files
- **threadPool :** number of threads for parallel conversion

```python
#convert NC files to parquet
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
```


## Self-service analysis

### Databricks database
The self service analysis are provided through delta lake tables defined as follow.

```python
tableName = 'satellite_carbon_dioxide'

#Read the satellites raw data
df = sqlContext.read.parquet("/mnt/datasets/satellite-carbon-dioxide/parquet/")

#Add an around 11km rounding column to unable data analysis on a geographic level on Power BI
df = df.withColumn("geoKey",f.concat(f.format_number(df.latitude, 1),f.lit('|'),f.format_number(df.longitude, 1)))\
        .withColumn("date",f.to_date(df.datetime))
df = df.alias('d').join(dfGeo,df.geoKey==dfGeo.geoKey).select('d.*','g.city','g.countryCode2','g.admin1','g.admin2')

#Save the data as delta files
df.write.option("mergeSchema", "true").mode("overwrite").partitionBy('countryCode2')\
                        .format("delta").save("%s/%s"%(databaseLocation,tableName))
#Create a spark table
spark.sql("DROP TABLE IF EXISTS %s.%s"%(databaseName,tableName))
spark.sql("CREATE TABLE %s.%s USING DELTA LOCATION '%s/%s'"%(databaseName,tableName,databaseLocation,tableName))

#Optimize the layout of Delta Lake data
spark.sql("OPTIMIZE SelfServiceWareHouse.satellite_carbon_dioxide ZORDER BY (admin2,admin1,city,date,co2)")
```
Delta tables allow us to use the delta cache system to provide faster query time for our self service model on Power BI.

### Power BI Direct Query Model

A data model is created in Power BI in order to materialize relationships between the databricks tables and allow interactive analysis.

![RelationShips](misc/Model%20Relationships.PNG)

The "Country detailed" table come directly from an [external website](https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv) in order to illustrate the ability to easily join external informations with databricks tables without the need to load them into the cluster. This hybrid model ability allow the user to go further with their analysis.

### Spark configuration

As cache was needed to unsure usability of the report, [configuration](clusters/cache_cluster.json) was applied to the cluster responsible to execute this workload.

```
"spark.databricks.io.cache.compression.enabled": "true",
"spark.databricks.io.cache.maxMetaDataCache": "10g",
"spark.databricks.io.cache.maxDiskUsage": "400g",
"spark.databricks.delta.preview.enabled": "true"
```

