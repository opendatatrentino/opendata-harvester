# Crawlers for OpenData providers

This project contains misc clients to APIs of data providers for the
[Open Data Trentino](https://github.com/opendatatrentino) project.

**Build status:**

| Branch | Status |
| ------ | ------ |
| master | [![Build Status](https://travis-ci.org/opendatatrentino/data-crawlers.svg?branch=master)](https://travis-ci.org/opendatatrentino/data-crawlers) |
| develop | [![Build Status](https://travis-ci.org/opendatatrentino/data-crawlers.svg?branch=develop)](https://travis-ci.org/opendatatrentino/data-crawlers) |


## Example usage

Download data to MongoDB:

```
harvester -vvv --debug crawl \
    --crawler pat_statistica \
	--storage mongodb+mongodb://database.local/harvester_data/statistica
```

```
harvester -vvv --debug crawl \
    --crawler pat_statistica_subpro \
	--storage mongodb+mongodb://database.local/harvester_data/statistica_subpro
```

Prepare data for insertion into ckan:

```
harvester -vvv --debug convert \
    --converter pat_statistica_to_ckan \
	--input mongodb+mongodb://database.local/harvester_data/statistica \
	--output mongodb+mongodb://database.local/harvester_data/statistica_clean
```

```
harvester -vvv --debug convert \
    --converter pat_statistica_subpro_to_ckan \
	--input mongodb+mongodb://database.local/harvester_data/statistica_subpro \
	--output mongodb+mongodb://database.local/harvester_data/statistica_subpro_clean
```

Actually load data to Ckan:

todo
