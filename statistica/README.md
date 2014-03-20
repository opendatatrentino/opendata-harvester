# Statistica

Download data from ``statweb.provincia.tn.it``.
We have two sets of datases:

- Indicatori strutturali
- Indicatori strutturali sub-provinciali


## Directory contents

```
client_statistica.py
    Client for the data provider API

data
    Directory containing downloaded data

download_statistica_140320.py
    New downloader script
.statistica-140320-rawdata.sqlite
    Output file for the above script

download_statistica_json.py
    New downloader scripts, storing downloaded data into
	json files. Intermediate step towards new downloader.

download_statistica.py
    Old downloader script, writing to statistica.db

download_statistica_subpro.py
    Old downloader script, writing to statistica_subpro.db

extract_info.py
    Old script to extract some information from statistica.db

generate_gexf.py
    Generate GEXF file from relationships of datasets in
	statistica.db (numeratore/denominatore relationships)
statistica.gexf
	Output file for the above script

README.md
    This file :)
```
