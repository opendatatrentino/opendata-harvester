from setuptools import setup, find_packages

version = '0.1a'

install_requires = [
    'cliff',  # for the CLI
    'pymongo',  # for the mongodb-based storage
    'requests',  # for crawlers
    'stevedore',  # to load plugins
]

entry_points = {
    'harvester.ext.crawlers': [
        'ckan = harvester.ext.crawlers.ckan:CkanCrawler',
        'pat_statistica = harvester_odt.pat_statistica.crawler:Statistica',
        'pat_statistica_subpro = harvester_odt.pat_statistica.crawler:StatisticaSubPro',  # noqa
    ],

    'harvester.ext.storage': [
        "jsondir = harvester.ext.storage.jsondir:JsonDirStorage",
        "memory = harvester.ext.storage.memory:MemoryStorage",
        "mongodb = harvester.ext.storage.mongodb:MongodbStorage",
        "sqlite = harvester.ext.storage.sqlite:SQLiteStorage",
    ],

    'harvester.ext.converters': [
        'pat_statistica_to_ckan = harvester_odt.pat_statistica.'
        'converter:StatisticaToCkan',

        'pat_statistica_subpro_to_ckan = harvester_odt.pat_statistica.'
        'converter:StatisticaSubProToCkan',
    ],

    'harvester.ext.importers': [
        "ckan = harvester.ext.importers.ckan:CkanImporter",
    ],

    'harvester.commands': [
        'list_crawlers = harvester.commands:ListCrawlers',
        'list_storages = harvester.commands:ListStorages',
        'list_converters = harvester.commands:ListConverters',
        'list_importers = harvester.commands:ListImporters',
        'crawl = harvester.commands:Crawl',
        'convert = harvester.commands:Convert',
        'import = harvester.commands:Import',
    ],

    'console_scripts': [
        'harvester = harvester.cli:main',
    ]
}

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
