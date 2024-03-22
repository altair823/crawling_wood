import sqlite3


class SqliteSet:
    def __init__(self, db_name, table_name):
        self.conn = sqlite3.connect(db_name)
        self.c = self.conn.cursor()
        self.table_name = table_name
        self.c.execute(f'CREATE TABLE IF NOT EXISTS {self.table_name} (element TEXT PRIMARY KEY)')

    def add(self, element):
        try:
            self.c.execute(f'INSERT INTO {self.table_name} VALUES (?)', (element,))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass  # 이미 원소가 존재하면 무시

    def remove(self, element):
        self.c.execute(f'DELETE FROM {self.table_name} WHERE element = ?', (element,))
        self.conn.commit()

    def contains(self, element):
        self.c.execute(f'SELECT element FROM {self.table_name} WHERE element = ?', (element,))
        return self.c.fetchone() is not None

    def get_all(self):
        self.c.execute(f'SELECT element FROM {self.table_name}')
        return [row[0] for row in self.c.fetchall()]
