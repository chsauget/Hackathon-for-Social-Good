# Hackathon for Social Good
Repository for the Databricks Hackathon !

## Installation

- Create an Azure Databricks workspace
- Create support ressources : Azure Storage, Azure Key Vault
- Create the Azure Key-Vault backed secret scope in the Databricks workspace [[documentation](https://docs.microsoft.com/fr-fr/azure/databricks/security/secrets/secret-scopes#create-an-azure-key-vault-backed-secret-scope)]
- Create a Databricks cluster
- Run [initialization notebooks](/notebooks/initialization)

## Usage

### Required librairies
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

### Self-service analysis
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
```
Delta tables allow us to use the delta cache system to provide faster query time for our self service model on Power BI.

A data model is created in Power BI in order to materialize relationships between the databricks tables and allow interactive analysis.

![RelationShips](misc/Model%20Relationships.PNG)

The "Country detailed" table come directly from an [external website](https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv) in order to illustrate the ability to easily join external informations with databricks tables without the need to load them into the cluster. This hybrid model ability allow the user to go further with their analysis.


