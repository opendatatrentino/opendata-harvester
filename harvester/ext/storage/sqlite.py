"""
SQLite-based storage.

- Documents are stored in a key/value table, as serialized json (TEXT field)
- Blobs are stored in a key/value table (BLOB field)
- Keyvals are stored in a key/value table, as serialized json (TEXT field)
"""

import sqlite3
import re
import json
import urlparse

from .base import (BaseStorage, BaseDocumentBucket, BaseBlobBucket,
                   BaseKeyvalBucket, NotFound)


class SQLiteStorage(BaseStorage):
    def __init__(self, *a, **kw):
        super(SQLiteStorage, self).__init__(*a, **kw)

        # Extract destination filename from URL
        # Storage URLs might be like:
        #     sqlite:///path/to/db.sqlite
        #     sqlite+file:///path/to/db.sqlite

        self._filename = self._get_file_name(self.url)
        self._existing_tables = None

    def _get_file_name(self, url):
        if not url:
            return ':memory:'

        parsed = urlparse.urlparse(url)
        if parsed.scheme not in ('file', 'sqlite'):
            raise ValueError(
                "Invalid SQLite url: {0!r} (invalid scheme)".format(url))

        if parsed.netloc:
            raise ValueError(
                "Invalid SQLite url: {0!r} (cannot define netloc)".format(url))

        return parsed.path

    # Required methods
    # ----------------

    # def list_object_types(self):
    #     query = 'SELECT name FROM "sqlite_master" where type=\'table\';'
    #     return list(x['name'] for x in self._query(query))

    # def list_objects(self, obj_type):
    #     self._check_table_name(obj_type)
    #     query = 'SELECT id FROM "{0}";'.format(obj_type)
    #     return list(x['id'] for x in self._query(query))

    # def get_object(self, obj_type, obj_id):
    #     self._check_table_name(obj_type)
    #     obj = self._query_one(
    #         'SELECT * FROM "{0}" WHERE id=?;'.format(obj_type),
    #         (obj_id,))
    #     return json.loads(obj['json_data'])

    # def set_object(self, obj_type, obj_id, obj):
    #     self._check_table_name(obj_type)
    #     self._ensure_table(obj_type)
    #     self._execute(
    #         'DELETE FROM "{0}" WHERE id=?;'.format(obj_type), (obj_id,))
    #     query = ('INSERT INTO "{0}" (id, json_data) VALUES (?, ?);'
    #              .format(obj_type))
    #     self._execute(query, (obj_id, json.dumps(obj)))
    #     self._commit()

    # def del_object(self, obj_type, obj_id):
    #     self._check_table_name(obj_type)
    #     self._execute('DELETE FROM "{0}" WHERE id=?;'.format(obj_type),
    #                   (obj_id,))

    # Custom methods
    # --------------

    @property
    def _connection(self):
        if getattr(self, '_cached_connection', None) is None:
            self._cached_connection = sqlite3.connect(self._filename)
            self._cached_connection.row_factory = sqlite3.Row
        return self._cached_connection

    def _cursor(self, *a, **kw):
        return self._connection.cursor(*a, **kw)

    def _execute(self, *a, **kw):
        return self._cursor().execute(*a, **kw)

    def _query(self, *a, **kw):
        cur = self._cursor()
        cur.execute(*a, **kw)
        return cur.fetchall()

    def _query_one(self, *a, **kw):
        cur = self._cursor()
        cur.execute(*a, **kw)
        return cur.fetchone()

    def _commit(self):
        self._connection.commit()

    def _list_tables(self):
        query = 'SELECT name FROM "sqlite_master" where type=\'table\';'
        return list(x['name'] for x in self._query(query))

    def _ensure_table(self, bucket_type, name):
        table_name = '{0}_{1}'.format(bucket_type, name)
        self._check_table_name(table_name)

        # We assume tables are *never* deleted, so we can keep
        # a list of existing table names, to speed up things by
        # avoiding excessive querying..

        if self._existing_tables is None:
            self._existing_tables = set(self._list_tables())

        if name in self._existing_tables:
            # We assume the table is there..
            return

        # Create table
        self._create_table(bucket_type, name)

        # Update cached list
        self._existing_tables = set(self._list_tables())

    def _create_table(self, bucket_type, name):
        table_name = '{0}_{1}'.format(bucket_type, name)
        self._check_table_name(table_name)

        if bucket_type == 'blob':
            query = """
            CREATE TABLE IF NOT EXISTS "{0}"
            ( id VARCHAR(256) PRIMARY KEY, value BLOB );
            """
        else:
            query = """
            CREATE TABLE IF NOT EXISTS "{0}"
            ( id VARCHAR(256) PRIMARY KEY, value TEXT );
            """

        c = self._cursor()
        c.execute(query.format(table_name))
        self._commit()

    def _check_table_name(self, name):
        if not re.match(r'^[A-Za-z0-9_]+$', name):
            raise ValueError("Invalid table name")


class BaseSQLiteBucket(object):
    bucket_type = None

    @classmethod
    def list_buckets(cls, storage):
        query = 'SELECT name FROM "sqlite_master" where type=\'table\';'
        _prefix = cls.bucket_type + '_'
        for row in storage._query(query):
            name = row['name']
            if name.startswith(_prefix):
                yield name[len(_prefix):]

    def _get_table_name(self):
        tn = '_'.join((self.bucket_type, self.name))
        self.storage._check_table_name(tn)
        return tn

    def __iter__(self):
        tbl = self._get_table_name()
        query = 'SELECT id FROM "{0}";'.format(tbl)

        try:
            result = self.storage._query(query)
        except sqlite3.OperationalError, e:
            if e.message.startswith('no such table'):
                result = []
            else:
                raise

        for row in result:
            yield row['id']

    def __len__(self):
        tbl = self._get_table_name()
        query = 'SELECT count(*) FROM "{0}";'.format(tbl)
        try:
            return self.storage._query_one(query)[0]
        except sqlite3.OperationalError, e:
            if e.message.startswith('no such table'):
                return 0
            raise

    def __getitem__(self, name):
        tbl = self._get_table_name()
        query = 'SELECT * FROM "{0}" WHERE id=?;'.format(tbl)

        try:
            obj = self.storage._query_one(query, (name,))

        except sqlite3.OperationalError, e:
            if e.message.startswith('no such table'):
                raise NotFound('object not found (no table)')
            else:
                raise

        if not obj:
            raise NotFound('object not found')

        return self._deserialize(obj['value'])

    def __setitem__(self, objid, obj):
        self.storage._ensure_table(self.bucket_type, self.name)
        tbl = self._get_table_name()

        # Make sure it isn't in our way..
        self.__delitem__(objid)
        # query = 'DELETE FROM "{0}" WHERE id=?;'.format(tbl)
        # self.storage._execute(query, (objid,))

        # Insert the updated version
        query = ('INSERT INTO "{0}" (id, value) VALUES (?, ?);' .format(tbl))

        self.storage._execute(query, (objid, self._serialize(obj)))
        self.storage._commit()

    def __delitem__(self, objid):
        tbl = self._get_table_name()
        query = 'DELETE FROM "{0}" WHERE id=?;'.format(tbl)

        try:
            self.storage._execute(query, (objid,))

        except sqlite3.OperationalError, e:
            if not e.message.startswith('no such table'):
                raise

    def _serialize(self, val):
        return json.dumps(val)

    def _deserialize(self, val):
        return json.loads(val)


class SQLiteDocumentBucket(BaseSQLiteBucket, BaseDocumentBucket):
    bucket_type = 'document'


class SQLiteBlobBucket(BaseSQLiteBucket, BaseBlobBucket):
    bucket_type = 'blob'

    def _serialize(self, val):
        return val

    def _deserialize(self, val):
        return val


class SQLiteKeyvalBucket(BaseSQLiteBucket, BaseKeyvalBucket):
    bucket_type = 'keyval'


SQLiteStorage.document_bucket_class = SQLiteDocumentBucket
SQLiteStorage.blob_bucket_class = SQLiteBlobBucket
SQLiteStorage.keyval_bucket_class = SQLiteKeyvalBucket
