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


## Statistica: indicatori strutturali

- ``<base>?fmt=json`` returns a json list of datasets
- Extra metadata for each dataset can be retrieved at ``<base>?idind=<id>&fmt=json``
- Dataset metadata is linking to the "indicatore", "numeratore" and "denominatore"
  tables, each of them containing data.
- The ``Indicatore`` field links to a json object with a single key. That ket
  can be used as title for the dataset / main resource
- Titles for the "numeratore" / "denominatore" resources can be retrieved by
  getting the only key from objects pointed to by the URLs in ``TabNumeratore``
  and ``TabDenominatore``.

### Datasets index

URL: http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=json

```json
{
  "IndicatoriStrutturali": [
    {
      "Descrizione": "Popolazione in eta' pre-scolare",
      "DescrizioneEstesa": "Peso della popolazione in età pre-scolare sulla popolazione residente",
      "Fonte": "Elaborazioni: PAT - Servizio Statistica",
      "URL": "http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=json&idind=2",
      "id": "2"
    },
    {
      "Descrizione": "Popolazione in eta' anziana",
      "DescrizioneEstesa": "Peso degli anziani sulla popolazione residente",
      "Fonte": "Elaborazioni: PAT - Servizio Statistica",
      "URL": "http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=json&idind=4",
      "id": "4"
    },
    ...
  ]
}
```

### Dataset metadata

URL: http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=json&idind=2

```json
{
  "Popolazione in eta' pre-scolare - metadati": [
    {
      "Algoritmo": "Popolazione residente di 3-5 anni su popolazione residente totale * 100",
      "AnnoInizio": "1990",
      "Area": "",
      "ConfrontiTerritoriali": "Alto Adige, Nord-Est, Italia, UE-27, UE-15, Zona Euro",
      "Descrizione": "Peso della popolazione in età pre-scolare sulla popolazione residente",
      "Fenomeno": "Stock",
      "FreqAggiornamento": "Annuale",
      "Indicatore": "http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=json&idind=2&t=i",
      "IndicatoreCSV": "http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=csv&idind=2&t=i",
      "LivelloGeografico": "Provincia",
      "Note": "Si tratta della popolazione residente, calcolata al 31 dicembre dell'anno di riferimento.",
      "ProxDispDatoDefinitivo": "Agosto",
      "ProxDispDatoProvvisorio": "",
      "Settore": "Popolazione",
      "TabDenominatore": "http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=json&idind=2&t=d",
      "TabDenominatoreCSV": "http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=csv&idind=2&t=d",
      "TabNumeratore": "http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=json&idind=2&t=n",
      "TabNumeratoreCSV": "http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=csv&idind=2&t=n",
      "UM": "",
      "UltimoAggiornamento": "09/08/2013"
    }
  ]
}
```


## Statistica: indicatori strutturali sub-provinciali

### Datasets index

URL: http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx?fmt=json&list=i

```json
{
  "Lista indicatori strutturali SP": [
    {
      "Algoritmo": "Numero di posti disponibili nei nidi d'infanzia fratto popolazione residente tra 0 e 2 anni per 100",
      "AnnoBase": "",
      "AnnoInizio": "2009",
      "Area": "Società",
      "Descrizione": "Capacità ricettiva dei nidi d'infanzia",
      "DescrizioneEstesa": "Numero di posti disponibili nei nidi d'infanzia fratto popolazione residente tra 0 e 2 anni per 100",
      "DescrizioneTabDen": "Popolazione residente da 0 a 2 anni anni di età",
      "DescrizioneTabNum": "",
      "Fonte": "Servizio Statistica Provincia Autonoma di Trento",
      "FrequenzaAggiornamento": "Annuale",
      "Gruppo": "Famiglie e aspetti sociali",
      "LivelloGeograficoMinimo": "Comune",
      "NomeTabDen": "Sub_Popolazione_0-2",
      "NomeTabNum": "Sub_Nido_Infanzia_Posti",
      "Settore": "Istruzione e formazione",
      "TipoFenomento": "Stock",
      "TipoIndicatore": "R",
      "URLIndicatoreD": "http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx?idind=0&info=d&fmt=json",
      "URLTabDenMD": "http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx?ntab=Sub_Popolazione_0-2&info=md&fmt=json",
      "URLTabNumMD": "http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx?ntab=Sub_Nido_Infanzia_Posti&info=md&fmt=json",
      "UltimoAggiornamento": "",
      "UnitàMisura": "Per cento",
      "id": "0"
    },
	...
  ]
}
```

### Metadata: denominatore / numeratore

Linked by ``URLTabNumMD`` and ``URLTabDenMD`` fields.

URL: http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx?ntab=Sub_Popolazione_0-2&info=md&fmt=json

```json
{
  "Popolazione residente da 0 a 2 anni - metadati": [
    {
      "Area": "Società",
      "URLTabD": "http://www.statweb.provincia.tn.it/indicatoristrutturalisubpro/exp.aspx?ntab=Sub_Popolazione_0-2&info=d&fmt=json",
      "UltimoAggiornamento": "01/01/2012",
      "descrizione": "Popolazione residente da 0 a 2 anni",
      "nomeTabella": "Sub_Popolazione_0-2"
    }
  ]
}
```
