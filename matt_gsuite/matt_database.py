import sqlite3


class MattDatabase:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        if not self.is_table('balances'):
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
        # Create table
        self.conn.execute('''CREATE TABLE balances
                     (timestamp INTEGER, account_id TEXT, balance REAL)''')

    def insert_balance(self, timestamp, account_id, balance):
        print("Inserting: " + str(timestamp) + ", " + account_id + ", " + balance)
        balances = [(timestamp, account_id, balance)]
        self.conn.executemany('INSERT INTO balances (timestamp, account_id, balance) VALUES (?,?,?)', balances)

        # Save (commit) the changes
        self.conn.commit()

    def get_latest_balance(self, account_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT MAX(timestamp), account_id, balance FROM balances WHERE account_id = ?", (account_id,))
        return cursor.fetchone()
