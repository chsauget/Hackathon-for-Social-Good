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