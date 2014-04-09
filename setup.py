from setuptools import setup, find_packages

version = '0.1a'

install_requires = [
    'stevedore',  # to load plugins
    'pymongo',  # for the mongodb-based storage
    'cliff',  # for the CLI
    'requests',  # for crawlers
]

entry_points = {
    'harvester.ext.crawlers': [

        'pat_statistica = harvester.ext.crawlers.odt'
        '.pat_statistica:Statistica',

        'pat_statistica_subpro = harvester.ext.crawlers.odt'
        '.pat_statistica:StatisticaSubPro',
    ],
    'harvester.ext.storage': [
        "jsondir = harvester.ext.storage.jsondir:JsonDirStorage",
        "memory = harvester.ext.storage.memory:MemoryStorage",
        "mongodb = harvester.ext.storage.mongodb:MongodbStorage",
        "sqlite = harvester.ext.storage.sqlite:SQLiteStorage",
    ],
    'harvester.commands': [
        'list_crawlers = harvester.commands:ListCrawlers',
        'list_storages = harvester.commands:ListStorages',
        'crawl = harvester.commands:Crawl',
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
