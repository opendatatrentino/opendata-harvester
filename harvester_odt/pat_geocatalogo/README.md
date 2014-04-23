# Geocatalogo PAT

The "geocatalogo" is a CSW server apparently implementing the
[INSPIRE](http://en.wikipedia.org/wiki/INSPIRE) initiative directive.

Base URL for the service is http://www.territorio.provincia.tn.it/geoportlet/srv/eng/csw

## CSW usage

Instantiate the client:

```python
from owslib.csw import CatalogueServiceWeb
csw = CatalogueServiceWeb('http://www.territorio.provincia.tn.it/geoportlet/srv/eng/csw')
```

Query:

```python
csw.getrecords2(resulttype='results_with_summary', startposition=0, maxrecords=10)
```

Get pagination information:

```python
csw.results
```
```python
{'matches': 168, 'nextrecord': 11, 'returned': 10}
```

Get raw response XML:

```python
csw.response
```
```xml
<?xml version="1.0" encoding="UTF-8"?>
<csw:GetRecordsResponse>
  <csw:SearchStatus timestamp="2014-04-23T09:28:09" />
  <geonet:Summary xmlns:geonet="http://www.fao.org/geonetwork" count="168" type="local">
	...
  </geonet:Summary>
  <csw:SearchResults numberOfRecordsMatched="168" numberOfRecordsReturned="10" elementSet="summary" nextRecord="11">
    <csw:SummaryRecord>
      <dc:identifier>p_tn:Bacino_Torrente_Noce</dc:identifier>
      <dc:title>IFF2007 nel BACINO DEL TORRENTE NOCE</dc:title>
      <dc:type>dataset</dc:type>
      <dc:subject>conservazione ambientale</dc:subject>
      <dc:subject>paesaggio</dc:subject>
      <dc:subject>qualità dell'acqua</dc:subject>
      <dc:subject>uso del suolo</dc:subject>
      <dc:subject>indice di funzionalità fluviale</dc:subject>
      <dc:subject>Idrografia</dc:subject>
      <dc:subject>inlandWaters</dc:subject>
      <dc:subject>environment</dc:subject>
      <dc:subject>planningCadastre</dc:subject>
      <dc:format>Shapefile</dc:format>
      <dc:relation>p_tn:Bacino_Torrente_Noce</dc:relation>
      <dct:modified>2013-06-28</dct:modified>
      <dct:abstract>La previsione di ambiti fluviali nel Piano Generale...</dct:abstract>
      <geonet:info>
        <id>1919</id>
        <schema>iso19139</schema>
        <createDate>2013-07-08T02:46:42</createDate>
        <changeDate>2013-07-08T13:47:32</changeDate>
        <isTemplate>n</isTemplate>
        <title />
        <source>de555167-3cf2-4b6d-9d80-df646efde40d</source>
        <uuid>3efe23ac-d252-4a59-95d1-2cf9c97b9e83</uuid>
        <isHarvested>n</isHarvested>
        <popularity>28</popularity>
        <rating>0</rating>
        <status>2</status>
        <rootid>1919</rootid>
        <parentid>-1</parentid>
        <islast>y</islast>
        <datastatus>1</datastatus>
        <guid>g1919</guid>
        <motivation>n</motivation>
        <flag>2</flag>
        <is_ogd>y</is_ogd>
        <ogd_xml>http://www.territorio.provincia.tn.it/geodati/1919_IFF2007_nel_BACINO_DEL_TORRENTE_NOCE_15_07_2013.xml</ogd_xml>
        <ogd_zip>http://www.territorio.provincia.tn.it/geodati/1919_IFF2007_nel_BACINO_DEL_TORRENTE_NOCE_15_07_2013.zip</ogd_zip>
        <ogd_rdf />
        <licenseType>1</licenseType>
        <uuidEscaped>3efe23ac-d252-4a59-95d1-2cf9c97b9e83</uuidEscaped>
        <view>true</view>
        <notify>false</notify>
        <download>true</download>
        <dynamic>true</dynamic>
        <featured>true</featured>
        <ownername>FRANCESCON MAURIZIO</ownername>
        <category>openData</category>
        <subtemplates />
        <groups>
          <record>
            <name>all</name>
          </record>
          <record>
            <name>APPA</name>
          </record>
        </groups>
      </geonet:info>
    </csw:SummaryRecord>
	...
  </csw:SearchResults>
</csw:GetRecordsResponse>
```
(see ``docs/search_result.xml`` for the complete response)

Extract results from the search XML:

```python
tree = lxml.etree.fromstring(client.response)
records = tree.xpath(
    '/csw:GetRecordsResponse/csw:SearchResults/csw:SummaryRecord',
	namespaces=tree.nsmap)
```
