import sqlite3
from file_meta import FileEntry


class MattDatabase:
    TABLE_NAME_BALANCE = 'balance'
    COLS_BALANCE = ('ts', 'account_id', 'bal')
    TABLE_FILE_LOG = 'file_log'
    COLS_FILE_LOG = ('sig', 'len', 'ts', 'path', 'deleted')

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        if not self.is_table(self.TABLE_NAME_BALANCE):
            self.create_tables()

    @staticmethod
    def build_insert(table_name, field_names):
        return 'INSERT INTO ' + table_name + '(' + ','.join(field_names) + ') VALUES (' + ','.join('?' for name in field_names) + ')'

    @staticmethod
    def build_select(table_name, field_names):
        return 'SELECT ' + ','.join(field_names) + ' FROM ' + table_name

    def is_table(self, table_name):
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + table_name + "';"
        cursor = self.conn.execute(query)
        result = cursor.fetchone()
        if result is None:
            print('Table does not exist')
            return False
        print('Table exists!')
        return True

    def close(self):
        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        self.conn.close()

    def create_tables(self):
        self.conn.execute('''CREATE TABLE balance(
                    ts INTEGER,
                    account_id TEXT,
                    bal INTEGER
                    )''')

        # deleted=1 if deleted
        self.conn.execute('''CREATE TABLE file_log(
                    sig TEXT,
                    length INTEGER,
                    sync_ts INTEGER,
                    file_path TEXT,
                    deleted INTEGER
                    )''')

    def insert_balance(self, timestamp, account_id, balance):
        print("Inserting: " + str(timestamp) + ", " + account_id + ", " + balance)
        balances = [(timestamp, account_id, balance)]
        sql = self.build_insert(self.TABLE_NAME_BALANCE, self.COLS_BALANCE)
        self.conn.executemany(sql, balances)

        # Save (commit) the changes
        self.conn.commit()

    def get_latest_balance(self, account_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT MAX(ts), account_id, balance FROM balance WHERE account_id = ?", (account_id,))
        return cursor.fetchone()

    def get_file_changes(self):
        cursor = self.conn.cursor()
        sql = self.build_select(self.TABLE_FILE_LOG, self.COLS_FILE_LOG)
        cursor.execute(sql)
        changes = cursor.fetchall()
        entries = []
        for change in changes:
            entries.append(FileEntry(change[0], change[1], change[2], change[3], change[4]))
        return entries

    # Takes a list of FileEntry objects:
    def insert_file_changes(self, entries):
        to_insert = []
        for entry in entries:
            to_insert.append(tuple(entry))
        sql = self.build_insert(self.TABLE_FILE_LOG, self.COLS_FILE_LOG)
        self.conn.executemany(sql, to_insert)
