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
        return self.name == other.name

