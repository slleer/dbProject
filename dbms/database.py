# Author: Stephen Leer
# date: 11/02/21
# version: 1.3
from table import Table


class Database:

    def __init__(self, db):
        self.name = db
        self.table = []

    def add(self, tbl):
        self.table.append(tbl)

    def remove(self, tbl):
        if tbl in self.table:
            self.table.remove(tbl)
        else:
            print("!Failed to delete", tbl, "because it does not exist.", sep=' ')
             
    def __eq__(self, other):
        if isinstance(other, str):
            return self.name.lower() == other.lower()
        elif isinstance(other, Database):
            return self.name.lower() == other.name.lower()

    def __str__(self):
        return self.name

