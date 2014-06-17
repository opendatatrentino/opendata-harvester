# Harvesting for ODT

We need to harvest the following sources:

- pat_statistica
- pat_statistica_subpro
- pat_geocatalogo

..into MongoDB; then we clean the data and import in Ckan.

**Note:** The first time we import datasets in the production
catalog, we'll need to find & add references to the old datasets.


## Script to run all the crawlers + converters

```bash
#!/bin/bash

MONGODB=mongodb://database.local/harvester_"$( date +"%Y-%m-%d" )"
HARVESTER=harvester

set -e    # Die on error
set -v    # Verbose

# First, run the crawlers
# --------------------------------------------------

harvester crawl \
    --crawler pat_statistica \
    --storage "${MONGODB}"/pat_statistica \
	--storage-option clean_first=true

harvester crawl \
    --crawler pat_statistica_subpro \
    --storage "${MONGODB}"/pat_statistica_subpro \
	--storage-option clean_first=true

harvester crawl \
    --crawler pat_geocatalogo \
    --storage "${MONGODB}"/pat_geocatalogo \
	--storage-option clean_first=true

harvester crawl \
    --crawler comunweb+http://www.comune.trento.it/ \
    --storage "${MONGODB}"/cweb_trento \
	--storage-option clean_first=true

# Then, run the converters
# --------------------------------------------------

harvester convert \
    --converter pat_statistica_to_ckan \
    --input "${MONGODB}"/pat_statistica \
	--output "${MONGODB}"/pat_statistica_clean \
    --output-option clean_first=true

harvester convert \
    --converter pat_statistica_subpro_to_ckan \
    --input "${MONGODB}"/pat_statistica_subpro \
	--output "${MONGODB}"/pat_statistica_subpro_clean \
    --output-option clean_first=true

harvester convert \
    --converter pat_geocatalogo_to_ckan \
    --input "${MONGODB}"/pat_geocatalogo \
	--output "${MONGODB}"/pat_geocatalogo_clean \
    --output-option clean_first=true

harvester convert \
    --converter comunweb_to_ckan \
    --converter-option org_name=comune-di-trento \
	--converter-option org_title="Comune di Trento" \
	--input "${MONGODB}"/cweb_trento \
	--output "${MONGODB}"/cweb_trento_clean \
	--output-option clean_first=true
```

**Note:** if you are reusing the same database, append
  ``--storage-option clean_first=true`` to the crawler arguments to
  make sure it is emptied before running, otherwise you'll get stale
  data hanging around.


## Using crawlers

The main command is ``harvester crawl``.

Relevant options:

- ``--crawler`` the crawler name. Use ``harvester list crawlers`` to
  see which crawlers are available.

- To list crawler-specific options, use ``harvester show crawler <name>``

- ``--storage`` specify the output storage. Use ``harvester list
  storages`` to list available storages, and ``harvester show storage
  <name>`` to see specific options.

- ``--storage-option clean_first=true`` make sure the destination
  database is empty before proceeding. Useful when reusing the same
  database multiple times.


## Using converters

Converters take data from a storage, prepare for insertion to
somewhere and write to another storage.

The main command is ``harvester convert``.

Relevant options:

- ``--converter`` specify the converter plugin to use. Use ``harvester
  list converters`` for a list of available converters.

- To list converter-specific options, use ``harvester show converter <name>``.

- ``--input`` specify the input storage for the
  conversion. ``--input-option`` is used to specify storage options.

- ``--output`` specify the output storage. Use ``harvester list
  storages`` to list available storages, and ``harvester show storage
  <name>`` to see specific options.

- ``--output-option clean_first=true`` make sure the destination
  database is empty before proceeding. Useful when reusing the same
  database multiple times.


## Importers

```bash
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


## Other utilities


### Linking datasets in catalog with ones from source

Configure variables in the script and launch:

```
python scripts/link-statistica-datasets-to-ckan.py
```

### Removing all the datasets for an organization

```
export CKAN_URL=...
export CKAN_API_KEY=...
./scripts/delete-old-datasets.py org-1-name org-2-name ...
```

The script will prompt interactively before proceeding with actions,
so you have the chance to review what will happen
