from setuptools import setup, find_packages

version = '0.2a'

install_requires = [
    'cliff',  # for the CLI
    'pymongo',  # for the mongodb-based storage
    'requests',  # for crawlers
    'stevedore',  # to load plugins
    'owslib',  # for geocatalogo CSW
    'lxml',  # for geocatalogo CSW
    'unidecode',  # to transliterate characters
    'termcolor',  # for colored logs
    'prettytable',  # for nicely formatted ASCII tables
]

entry_points = {
    'harvester.ext.crawler': [
        'ckan = harvester.ext.crawler.ckan:CkanCrawler',
    ],

    'harvester.ext.storage': [
        "jsondir = harvester.ext.storage.jsondir:JsonDirStorage",
        "memory = harvester.ext.storage.memory:MemoryStorage",
        "mongodb = harvester.ext.storage.mongodb:MongodbStorage",
        "sqlite = harvester.ext.storage.sqlite:SQLiteStorage",
    ],

    'harvester.ext.converter': [
    ],

    'harvester.ext.importer': [
        "ckan = harvester.ext.importer.ckan:CkanImporter",
    ],

    'harvester.commands': [
        'list_crawlers = harvester.commands:ListCrawlers',
        'list_storages = harvester.commands:ListStorages',
        'list_converters = harvester.commands:ListConverters',
        'list_importers = harvester.commands:ListImporters',
        'show_crawler = harvester.commands:ShowCrawler',
        'show_storage = harvester.commands:ShowStorage',
        'show_converter = harvester.commands:ShowConverter',
        'show_importer = harvester.commands:ShowImporter',
        'crawl = harvester.commands:Crawl',
        'convert = harvester.commands:Convert',
        'import = harvester.commands:Import',
        'storage_inspect = harvester.commands:StorageInspect',
    ],

    'console_scripts': [
        'harvester = harvester.cli:main',
    ]
}

entry_points_odt = {
    'harvester.ext.crawler': [
        'pat_statistica = harvester_odt.pat_statistica.crawler:Statistica',
        'pat_statistica_subpro = harvester_odt.pat_statistica.crawler:StatisticaSubPro',  # noqa
        'pat_geocatalogo = harvester_odt.pat_geocatalogo.crawler:Geocatalogo',
    ],

    'harvester.ext.converter': [
        'pat_statistica_to_ckan = harvester_odt.pat_statistica.'
        'converter:StatisticaToCkan',
        'pat_statistica_subpro_to_ckan = harvester_odt.pat_statistica.'
        'converter:StatisticaSubProToCkan',
        'pat_geocatalogo_to_ckan = harvester_odt.pat_geocatalogo.'
        'converter:GeoCatalogoToCkan',
    ],
}

for epname, eplist in entry_points_odt.iteritems():
    if epname not in entry_points:
        entry_points[epname] = []
    entry_points[epname].extend(eplist)


setup(
    name='harvester',
    version=version,
    packages=find_packages(),
    url='http://opendatatrentino.github.io/harvester',
    license='BSD License',
    author='Samuele Santi',
    author_email='s.santi@trentorise.eu',
    description='',
    long_description='',
    install_requires=install_requires,
    # tests_require=tests_require,
    test_suite='harvester.tests',
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
    ],
    package_data={'': ['README.md', 'LICENSE']},
    include_package_data=True,
    zip_safe=False,
    entry_points=entry_points)
