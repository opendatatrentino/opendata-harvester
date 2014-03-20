import json
import re
import sqlite3


class SQLiteKeyValueStore(object):
    """SQLite-backed key/value store for data"""

    def __init__(self, filename):
        self.filename = filename

    @property
    def connection(self):
        if getattr(self, '_connection', None) is None:
            self._connection = sqlite3.connect(self.filename)
            self._connection.row_factory = sqlite3.Row
        return self._connection

    def cursor(self, *a, **kw):
        return self.connection.cursor(*a, **kw)

    def execute(self, *a, **kw):
        return self.cursor().execute(*a, **kw)

    def query(self, *a, **kw):
        cur = self.cursor()
        cur.execute(*a, **kw)
        return cur.fetchall()

    def query_one(self, *a, **kw):
        cur = self.cursor()
        cur.execute(*a, **kw)
        return cur.fetchone()

    def commit(self):
        self.connection.commit()

    def create_tables(self):
        pass

    def _create_table(self, name, numeric_key=True):
        self._check_table_name(name)
        id_type = 'INT' if numeric_key else 'VARCHAR(128)'
        c = self.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS "{0}"
        ( id {1} PRIMARY KEY, json_data TEXT );
        """.format(name, id_type))
        self.commit()

    def _check_table_name(self, name):
        if not re.match(r'^[A-Za-z0-9_]+$', name):
            raise ValueError("Invalid table name")

    def get(self, table, id):
        self._check_table_name(table)
        return self.query_one(
            'SELECT * FROM "{0}" WHERE id=?;'.format(table), (id,))

    def get_all(self, table):
        self._check_table_name(table)
        for row in self.query('SELECT * FROM "{0}";'.format(table)):
            yield json.loads(row['json_data'])

    def set(self, table, key, record):
        self._check_table_name(table)
        self.execute('DELETE FROM "{0}" WHERE id=?;'.format(table),
                     (key,))
        query = ('INSERT INTO "{0}" (id, json_data) VALUES (?, ?);'
                 .format(table))
        self.execute(query, (key, json.dumps(record)))
        self.commit()
