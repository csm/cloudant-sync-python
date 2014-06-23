import sqlite3


class Database(object):
    def __init__(self, connection, on_stmt=None):
        if connection is None or not isinstance(connection, sqlite3.Connection):
            raise ValueError("expecting a sqlite3.Connection")
        self.__connection = connection
        self.__transaction_stack = []
        self.__transaction_set_success = False
        self.__cursor = None
        self.__on_stmt = on_stmt

    def execute(self, sql, params=()):
        if self.__on_stmt is not None:
            self.__on_stmt(sql, tuple(params))
        if self.__cursor is not None:
            self.__cursor.execute(sql, params)
            return self.__cursor
        else:
            return self.__connection.execute(sql, params)

    def __exec_stmt(self, sql, params=()):
        c = self.execute(sql, params)
        c.fetchall()
        return c

    def get_version(self):
        c = self.execute("PRAGMA user_version;")
        return c.fetchone()[0]

    def is_closed(self):
        return self.__connection is not None

    def close(self):
        if self.__connection is not None:
            self.__connection.close()
            self.__connection = None

    def begin_transaction(self):
        if len(self.__transaction_stack) == 0:
            self.__cursor = self.__connection.cursor()
            self.__transaction_set_success = True
        self.__transaction_stack.append(False)

    def end_transaction(self):
        success = self.__transaction_stack.pop()
        # print '[end_transaction] transaction stack: ' + str(self.__transaction_stack)
        if not success:
            self.__transaction_set_success = False
        if len(self.__transaction_stack) == 0:
            if self.__transaction_set_success:
                self.__connection.commit()
            else:
                self.__connection.rollback()
            self.__cursor = None

    def set_transaction_success(self):
        self.__transaction_stack[-1] = True
        # print 'transaction stack:', str(self.__transaction_stack)

    def compact_database(self):
        self.execute('VACUUM;')

    @staticmethod
    def __build_query(table, values, where_clause):
        query = 'UPDATE %s SET ' % table
        sep = ''
        for key in values.keys():
            query += '%s%s=?' % (sep, key)
            sep = ', '
        if where_clause is not None:
            query += ' WHERE %s' % where_clause
        return query

    def update(self, table, values, where_clause=None, where_args=()):
        if not isinstance(table, str) and not isinstance(table, unicode):
            raise ValueError('expecting a string table name')
        if not isinstance(values, dict):
            raise ValueError('expecting a dict of values')
        if where_clause is not None and not isinstance(where_clause, str) and not isinstance(where_clause, unicode):
            raise ValueError('expecting a string where clause (or None)')
        if where_args is None:
            where_args = ()
        changes = self.__connection.total_changes
        self.__exec_stmt(self.__build_query(table, values, where_clause), list(values.values()) + list(where_args))
        return self.__connection.total_changes - changes

    def delete(self, table, where_clause=None, where_args=()):
        query = 'DELETE FROM %s' % table
        if where_clause is not None:
            query += ' WHERE %s' % where_clause
        changes = self.__connection.total_changes
        self.__exec_stmt(query, where_args)
        return self.__connection.total_changes - changes

    def insert_with_on_conflict(self, table, values, on_conflict=''):
        query = 'INSERT %s INTO %s (' % (on_conflict, table)
        sep = ''
        for key in values.keys():
            query += '%s%s' % (sep, key)
            sep = ', '
        query += ') VALUES (%s)' % ','.join(['?' for _ in range(0, len(values))])
        cursor = self.__exec_stmt(query, values.values())
        return cursor.lastrowid

    def insert(self, table, values):
        return self.insert_with_on_conflict(table, values, '')