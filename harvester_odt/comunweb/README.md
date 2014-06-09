# Harvester for ComunWeb

## Test runs

```bash
MONGODB=mongodb://database.local/harvester-"$( date +"%Y-%m-%d" )"
```

**Crawl:**

```bash
ipdb $( which harvester ) --debug -vvv crawl --crawler comunweb+http://www.comune.trento.it/ --storage "${MONGODB}"/cweb_trento --storage-option clean_first=true
```

**Convert:**
```bash
ipdb $( which harvester ) --debug -vvv convert --converter comunweb_to_ckan --converter-option org_name=comune-di-trento --converter-option org_title="Comune di Trento" --input "${MONGODB}"/cweb_trento --output "${MONGODB}"/cweb_trento_clean --output-option clean_first=true
```
