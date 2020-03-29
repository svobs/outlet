import sqlite3
from fmeta.fmeta import FMeta


class MattDatabase:
    TABLE_BALANCE = {
        'name': 'balance',
        'cols': (('ts', 'INTEGER'),
                  ('account_id', 'TEXT'),
                  ('bal', 'INTEGER'))
    }
    TABLE_FILE_LOG = {
        'name': 'file_log',
        'cols': (('sig', 'TEXT'),
                ('len', 'INTEGER'),
                ('sync_ts', 'INTEGER'),
                ('modify_ts', 'INTEGER'),
                ('path', 'TEXT'),
                ('status', 'TEXT'))
    }

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        if not self.is_table(self.TABLE_BALANCE):
            self.create_tables()

    # Utility Functions ---------------------

    @staticmethod
    def build_insert(table):
        return 'INSERT INTO ' + table['name'] + '(' + ','.join(col[0] for col in table['cols']) + ') VALUES (' + ','.join('?' for col in table['cols']) + ')'

    @staticmethod
    def build_select(table):
        return 'SELECT ' + ','.join(col[0] for col in table['cols']) + ' FROM ' + table['name']

    @staticmethod
    def build_create_table(table):
        return 'CREATE TABLE ' + table['name'] + '(' + ', '.join(col[0] + ' ' + col[1] for col in table['cols']) + ')'

    def is_table(self, table):
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + table['name'] + "';"
        cursor = self.conn.execute(query)
        result = cursor.fetchone()
        return result is not None

    def close(self):
        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        self.conn.close()

    def create_tables(self):
        sql = self.build_create_table(self.TABLE_BALANCE)
        print('Executing SQL: ' + sql)
        self.conn.execute(sql)

        sql = self.build_create_table(self.TABLE_FILE_LOG)
        print('Executing SQL: ' + sql)
        self.conn.execute(sql)

    # BALANCE operations ---------------------

    def insert_balance(self, timestamp, account_id, balance):
        print("Inserting: " + str(timestamp) + ", " + account_id + ", " + balance)
        balances = [(timestamp, account_id, balance)]
        sql = self.build_insert(self.TABLE_BALANCE)
        self.conn.executemany(sql, balances)

        # Save (commit) the changes
        self.conn.commit()

    def get_latest_balance(self, account_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT MAX(ts), account_id, bal FROM balance WHERE account_id = ?", (account_id,))
        return cursor.fetchone()

    # FILE_LOG operations ---------------------

    # Hella janky way to see if our database contains real data. Needs improvement!
    def has_file_changes(self):
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_FILE_LOG) + ' LIMIT 1'
        cursor.execute(sql)
        changes = cursor.fetchall()
        return len(changes) > 0

    # Gets all changes in the table
    def get_file_changes(self):
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_FILE_LOG)
        cursor.execute(sql)
        changes = cursor.fetchall()
        entries = []
        for change in changes:
            entries.append(FMeta(change[0], change[1], change[2], change[3], change[4], change[5]))
        return entries

    # Takes a list of FMeta objects:
    def insert_file_changes(self, entries):
        to_insert = []
        for entry in entries:
            to_insert.append(tuple(entry))
        sql = self.build_insert(self.TABLE_FILE_LOG)
        self.conn.executemany(sql, to_insert)
        self.conn.commit()
