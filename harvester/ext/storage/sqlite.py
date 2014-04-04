import sqlite3
import re
import json

from .base import BaseStorage


class SQLiteStorage(BaseStorage):
    def __init__(self, filename):
        self._filename = filename
        self._existing_tables = None

    # Required methods
    # ----------------

    def list_object_types(self):
        query = 'SELECT name FROM "sqlite_master" where type=\'table\';'
        return list(x['name'] for x in self._query(query))

    def list_objects(self, obj_type):
        self._check_table_name(obj_type)
        query = 'SELECT id FROM "{0}";'.format(obj_type)
        return list(x['id'] for x in self._query(query))

    def get_object(self, obj_type, obj_id):
        self._check_table_name(obj_type)
        obj = self._query_one(
            'SELECT * FROM "{0}" WHERE id=?;'.format(obj_type),
            (obj_id,))
        return json.loads(obj['json_data'])

    def set_object(self, obj_type, obj_id, obj):
        self._check_table_name(obj_type)
        self._ensure_table(obj_type)
        self._execute(
            'DELETE FROM "{0}" WHERE id=?;'.format(obj_type), (obj_id,))
        query = ('INSERT INTO "{0}" (id, json_data) VALUES (?, ?);'
                 .format(obj_type))
        self._execute(query, (obj_id, json.dumps(obj)))
        self._commit()

    def del_object(self, obj_type, obj_id):
        self._check_table_name(obj_type)
        self._execute('DELETE FROM "{0}" WHERE id=?;'.format(obj_type),
                      (obj_id,))

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

    def _ensure_table(self, name):
        self._check_table_name(name)
        # we cache the list as this function will be called many times
        if self._existing_tables is None:
            self._existing_tables = set(self.list_object_types())
        if name not in self._existing_tables:
            self._create_table(name)
            self._existing_tables.add(name)

    def _create_table(self, name, numeric_key=False):
        self._check_table_name(name)
        id_type = 'INT' if numeric_key else 'VARCHAR(128)'
        c = self._cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS "{0}"
        ( id {1} PRIMARY KEY, json_data TEXT );
        """.format(name, id_type))
        self._commit()

    def _check_table_name(self, name):
        if not re.match(r'^[A-Za-z0-9_]+$', name):
            raise ValueError("Invalid table name")
