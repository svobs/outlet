import sqlite3
from file_meta import FileEntry

class MattDatabase:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        if not self.is_table('balance'):
            self.create_tables()

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
                    balance INTEGER
                    )''')

        # deleted=1 if deleted
        self.conn.execute('''CREATE TABLE file_log(
                    sig TEXT,
                    length INTEGER,
                    sync_ts INTEGER,
                    path TEXT,
                    deleted INTEGER
                    )''')

    def insert_balance(self, timestamp, account_id, balance):
        print("Inserting: " + str(timestamp) + ", " + account_id + ", " + balance)
        balances = [(timestamp, account_id, balance)]
        self.conn.executemany('INSERT INTO balance (ts, account_id, balance) VALUES (?,?,?)', balances)

        # Save (commit) the changes
        self.conn.commit()

    def get_latest_balance(self, account_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT MAX(ts), account_id, balance FROM balance WHERE account_id = ?", (account_id,))
        return cursor.fetchone()

    def get_file_changes(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT sig, length, sync_ts, path, deleted FROM file_log")
        changes = cursor.fetchall()
        entries = []
        for change in changes:
            entries.append(FileEntry(change[0], change[1], change[2], change[3], change[4]))
        return entries