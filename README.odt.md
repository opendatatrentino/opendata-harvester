# Harvesting for ODT

We need to harvest the following sources:

- pat_statistica
- pat_statistica_subpro
- pat_geocatalogo

..into MongoDB; then we clean the data and import in Ckan.

**Note:** The first time we import datasets in the production
catalog, we'll need to find & add references to the old datasets.


## Script to run all the stuff

```bash
#!/bin/bash

MONGODB=mongodb://database.local/harvester-"$( date +"%Y%m%d-%H%M%S" )"
HARVESTER=harvester

$HARVESTER crawl --crawler pat_statistica --storage "${MONGODB}"/pat_statistica
$HARVESTER crawl --crawler pat_statistica_subpro --storage "${MONGODB}"/pat_statistica_subpro
$HARVESTER crawl --crawler pat_geocatalogo --storage "${MONGODB}"/pat_geocatalogo

## Run converters

$HARVESTER convert --converter pat_statistica_to_ckan \
    --input "${MONGODB}"/pat_statistica --output "${MONGODB}"/pat_statistica_clean

$HARVESTER convert --converter pat_statistica_subpro_to_ckan \
    --input "${MONGODB}"/pat_statistica_subpro --output "${MONGODB}"/pat_statistica_subpro_clean

$HARVESTER convert --converter pat_geocatalogo_to_ckan \
    --input "${MONGODB}"/pat_geocatalogo --output "${MONGODB}"/pat_geocatalogo_clean

## Run the importer

CKAN_URL=http://dati.trentino.it
CKAN_API_KEY=1234-5678-...

$HARVESTER import --storage "${MONGODB}"/pat_statistica_clean \
    --importer ckan+"$CKAN_URL" \
	--importer-option api_key="$CKAN_API_KEY" \
	--importer-option source_name=statistica

$HARVESTER import --storage "${MONGODB}"/pat_statistica_subpro_clean \
    --importer ckan+"$CKAN_URL" \
	--importer-option api_key="$CKAN_API_KEY" \
	--importer-option source_name=statistica_subpro

$HARVESTER import --storage "${MONGODB}"/pat_geocatalogo_clean \
  --importer ckan+"$CKAN_URL" \
  --importer-option api_key="$CKAN_API_KEY" \
  --importer-option source_name=geocatalogo
```


## Linking datasets in catalog with ones from source

Configure variables in the script and launch:

```
python scripts/link-statistica-datasets-to-ckan.py
```

## Proposal: jobs director / orchestrator

- Handle jobs w/ mongodb storage
- We want to keep track of executed jobs (in a separate collection / db)
- We want to provide some "higher level" stuff on top of the harvester
