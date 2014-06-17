# OpenData Harvester

This project, developed as part of the [Open Data Trentino](https://github.com/opendatatrentino)
project, is a suite of tools to allow easy importing of batches of datasets from data providers
to data catalogs.

**Build status:**

| Branch | Status |
| ------ | ------ |
| master | [![Build Status](https://travis-ci.org/opendatatrentino/opendata-harvester.svg?branch=master)](https://travis-ci.org/opendatatrentino/opendata-harvester) |
| develop | [![Build Status](https://travis-ci.org/opendatatrentino/opendata-harvester.svg?branch=develop)](https://travis-ci.org/opendatatrentino/opendata-harvester) |

## Installation

Simply install the tarball from github:

```
pip install https://github.com/opendatatrentino/data-crawlers/tarball/master
```

if you plan to use it to import data to ckan, you'll need the [Ckan API client](https://github.com/opendatatrentino/ckan-api-client/) too. To install the stable version from pypi:

```
pip install ckan-api-client
```

Or the latest from git:

```
pip install http://git.io/ckan-api-client.tar.gz
```

## Concepts

This package will install a command-line script named ``harvester`` which
can be used to perform all the needed operations.

The command is extensible by using entry points to provide additional
plugins.

There are four plugin types that can be defined:

- storage -- abstraction for different types of storage
- crawler -- download data from source, store in storage
- converter -- convert data from a storage to another one
- importer -- import data from a storage to a catalog


## Core plugins

**Storages:**

- ``memory`` -- keep data in memory (mainly for testing)
- ``jsondir`` -- keep data as json files in a directory (for local testing)
- ``sqlite`` -- keep data in a sqlite database (for local testing)
- ``mongodb`` -- keep data in a mongodb database (recommended for production)


**Crawlers:**

- ``pat_statistica`` -- for ODT / servizio statistica
- ``pat_statistica_subpro`` -- for ODT / servizio statistica
- ``pat_geocatalogo`` -- for ODT / GeoCatalogo PAT
- ``comunweb`` -- for ComunWeb sites


**Converters:**

- ``pat_statistica_subpro_to_ckan`` -- for ODT / servizio statistica
- ``pat_statistica_to_ckan`` -- for ODT / servizio statistica
- ``pat_geocatalogo_to_ckan`` -- for ODT / GeoCatalogo PAT
- ``comunweb_to_ckan`` -- convert from ComunWeb to Ckan


**Importers:**

- ``ckan`` -- to import data into a [ckan](http://ckan.org) catalog.


## Example usage

Download data to MongoDB:

```
harvester -vvv --debug crawl \
    --crawler pat_statistica \
	--storage mongodb://database.local/harvester_data/statistica
```

```
harvester -vvv --debug crawl \
    --crawler pat_statistica_subpro \
	--storage mongodb://database.local/harvester_data/statistica_subpro
```

Prepare data for insertion into ckan:

```
harvester -vvv --debug convert \
    --converter pat_statistica_to_ckan \
	--input mongodb://database.local/harvester_data/statistica \
	--output mongodb://database.local/harvester_data/statistica_clean
```

```
harvester -vvv --debug convert \
    --converter pat_statistica_subpro_to_ckan \
	--input mongodb://database.local/harvester_data/statistica_subpro \
	--output mongodb://database.local/harvester_data/statistica_subpro_clean
```

Actually load data to Ckan:

```
harvester -vvv --debug import \
	--storage mongodb://database.local/harvester_data/statistica_clean \
	--importer ckan+http://127.0.0.1:5000 \
	--importer-option api_key=00112233-4455-6677-8899-aabbccddeeff \
	--importer-option source_name=statistica
```

```
harvester -vvv --debug import \
	--storage mongodb://database.local/harvester_data/statistica_subpro_clean \
	--importer ckan+http://127.0.0.1:5000 \
	--importer-option api_key=00112233-4455-6677-8899-aabbccddeeff \
	--importer-option source_name=statistica_subpro
```

## Running with debugger

Use something like this:

```
pdb $( which harvester ) -vvv --debug ....
```
