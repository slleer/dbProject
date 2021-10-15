# Author: Stephen Leer
# date: 9/29/21
# version: 2.3
import os
import sys
from database import Database
from table import Table


class DatabaseManagementSystem:
    #constructor
    def __init__(self):
        self.db = []
        self.cur_db = ''
        self.executeCommand = {0: self.createCommand,
                               1: self.alterCommand,
                               2: self.dropCommand,
                               3: self.updateCommand,
                               4: self.useCommand,
                               5: self.selectCommand,
                               6: self.insertCommand,
                               7: self.exitCommand,
                               8: self.listTables,
                               9: self.listDatabases}
        self.initializeDatabase()

    def listTables(self):
        if self.cur_db == "":
            print("No database selected, select database with USE command and try again.")
        else:
            cw_dir = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
            table_list = os.listdir(cw_dir)
            for tbl in table_list:
                print(tbl)

    def listDatabases(self):
        db_list = os.listdir(os.path.abspath(os.getcwd()))
        for db in db_list:
            if os.path.isdir(os.path.join(os.path.abspath(os.getcwd()), db)):
                print(db)

    def insertCommand(self, command):
        commands, table_data = self.get_insertion_values(command)
        if commands[1].lower() == "into" and commands[3].lower() == "values":
            table = commands[2]
            cw_dir = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
            cw_dir = os.path.join(cw_dir, table)
            # tbl = self.db[self.db.index(self.cur_db)] --------- need to refactor changing self.cur_db from str to db object. will make life lots easier
            for db in self.db:
                if db.name == self.cur_db:
                    for tbl in db.table:
                        if tbl.name == table:
                            if tbl.test_table_data(table_data):
                                with open(cw_dir, 'a') as table_to_append:
                                    table_to_append.write("\n")
                                    table_to_append.write(" | ".join(table_data))
                                    return
        else:
            print('!Failed to insert data, please review statement and try again.')


    def get_insertion_values(self, command):
        data = command[command.find("(") + 1:command.find(");")]
        command_vals = command[:command.find("(")]
        data = "".join(data.split())
        return command_vals.split(), data.split(',')

    # unused update command
    def updateCommand(self, command):
        pass


    # execution method, this method calls the appropriate method depending on
    # the index passed in. Command is the command received from user
    def execute(self, index, command):
        if index >= len(self.executeCommand)-3:
            self.executeCommand[index]()
        else:
            (self.executeCommand[index](command))
        
    # method responsible for displaying table contents to user
    def selectCommand(self, command):
        if not self.cur_db:
            print("No DATABASE selected, select DATABASE using USE command and try again")
        else:
            #try:
            file_path = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
            table_to_read = command.split()[command.split().index("from")+1].rstrip(';')
            if os.path.isfile(os.path.join(file_path, table_to_read)):
                file_path = os.path.join(file_path, table_to_read)
                if command.split()[1] == '*':
                    if "where" not in command.lower():
                        with open(file_path, 'r') as file_to_read:
                            print(file_to_read.read())
                    else:
                        print("can't handle conditionals yet.... please implement. ")
                else:
                    index = self.get_indices(command.split(), 1, command.split().index("from"))
                    conditionals = []
                    table_obj = None
                    for db in self.db:
                        if db.name == self.cur_db:
                            for tbl in db.table:
                                if tbl.name == table_to_read:
                                    table_obj = tbl
                    #if "where" not in command.lower():
                    command_column = []
                    for i in index:
                        command_column.append(command.split()[i].rstrip(","))
                    table_columns = self.get_indices_with_match(command_column, table_obj)

                    if "where" in command.lower():
                        condition_indices = self.get_indices(command, command.lower().split().index("where") + 1)
                        for i in condition_indices:
                            conditionals.append(command.split()[i].rstrip(";"))

                    with open(file_path, 'r') as file_to_read:
                        lines = file_to_read.readlines()[1:]
                        for line in lines:
                            if(not bool(conditionals) or self.condition_met(line.split(" | "), conditionals, table_obj)):
                                data_pieces = line.split("|")
                                print_str = []
                                for i in table_columns:
                                    print_str.append(data_pieces[i])
                                print(" | ".join(print_str))

                                # need to print only the pieces that match with the table columns indices returned


            else:
                print("!Failed to query table {} because it does not exist".format(table_to_read))
            #except:
                #print("Syntax error, please review statement and try again.")
    def condition_met(self, data, conditions, table):
        if len(conditions) == 3:
            column_index = self.get_indices_with_match(conditions, table)
            return table.check_condition(column_index[0], data[column_index[0]], conditions[1], conditions[2])

    def get_indices_with_match(self, columns, table):
        attribute_col_index = []
        index = 0
        for attribute in table.attributes:
            for col in columns:
                if col == attribute.attribute_name:
                    attribute_col_index.append(index)
                    print(index)
            index += 1
        return attribute_col_index

    def get_indices(self, command, lower_bound = None, upper_bound = None):
        indices = []
        if not lower_bound:
            lower_bound = 0
        if not upper_bound:
            upper_bound = len(command.split())
        for i in range(lower_bound, upper_bound):
            indices.append(i)
        return indices

    # method responsible for altering a table                            
    def alterCommand(self, command):
        if self.cur_db == '':
            print("No DATABASE selected, please select DATABASE with USE command and try agian")
        elif  len(command.split() < 5):
            print("Syntax error, please review statement and try again")
        elif command.split()[1].lower() == "table":
            table_to_alter = command.split()[2].rstrip(';')
            alteration = command.split()[3]
            if alteration.lower() == 'add':
                file_path = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
                if os.path.isfile(os.path.join(file_path, table_to_alter)):
                    table = os.path.join(file_path, table_to_alter)
                    with open(table, 'a') as f:
                        for alt in range(4, len(command.split())):
                            f.write(command.split()[alt].rstrip(';'))
                else:
                    print("!Failed to alter table", table_to_alter, "because it does not exist.", sep=' ')
        else:
            print("Syntax error, please review statement and try again")

    # method responsible for terminating the program
    def exitCommand(self):
        sys.exit()
    #method responsible for droping databases or tables

    def dropCommand(self, command):
        if len(command.split()) == 3:
            structureType = command.split()[1].lower()
            structureName = command.split()[2].rstrip(';')
            cw_dir = os.path.abspath(os.getcwd())
            if structureType == "database":
                if os.path.isdir(os.path.join(cw_dir, structureName)):
                    os.rmdir(os.path.join(cw_dir, structureName))
                    for db in self.db:
                       if db.name == structureName:
                           self.db.remove(db)
                    if self.cur_db == structureName:
                        self.cur_db = ''
                else:
                    print("!Failed ot delete DATABASE", structureName, "because it does not exist", sep=" ")
            elif structureType.lower() == "table":
                tbl_path = os.path.join(cw_dir, self.cur_db)
                print(tbl_path)
                if os.path.isfile(os.path.join(tbl_path, structureName)):
                    os.remove(os.path.join(tbl_path, structureName))
                    index = self.db.index(self.cur_db)
                    #self.db[index].remove(structureName)
                else:
                    print("!Failed to delete", structureName, "becasue it does not exist.", sep=" ")
            else:
                print("Syntax error, please review statement and try again. not table or database.")
        else:
            print("Syntax error, please review statement and try again.")

    #method responsible for selecting the database
    def useCommand(self, command):
        cw_dir = os.path.abspath(os.getcwd())
        if len(command.split()) == 2:
            db_to_use = command.split()[1].rstrip(';')
            if os.path.isdir(os.path.join(cw_dir, db_to_use)):
                print("USING", db_to_use, sep=' ')
                self.cur_db = db_to_use
            else:
                print("!Failed to USE DATABASE", db_to_use, "because it does not exist", sep=" ")
        else:
            print("Syntax error, please review statement and try again")

    # method responsible for controling creation of database or table by
    # calling the appropriate method and parsing the command as necessary
    def createCommand(self, command):
        if len(command.split()) < 3:
            print("Syntax error, please review the statement and try again.")
        else:
            structureType = command.split()[1] # determine if table or database
            structureName = command.split()[2] # get name of table or database
            if structureType.lower() == "database":
                new_db = self.createDB(structureName)
                self.db.append(new_db)
            elif structureType.lower() == "table":
                if not self.cur_db:
                    print("No DATABASE selected, select DATABASE with USE command and try again")
                elif self.verify_table_attributes(self.getTableAttributes(command)):
                    self.createTable(structureName, self.getTableAttributes(command), self.cur_db)
            else:
                print("Syntax error, please review statement and try again.")

    # method responsible for add existing databases and tables to the system
    # this method is still under development
    def initializeDatabase(self):
        cw_dir = os.path.abspath(os.getcwd())
        allDBs = os.listdir(cw_dir)
        for db in allDBs:
            if os.path.isdir(os.path.join(cw_dir, db)) and db[0].isalnum():
                self.db.append(Database(db))
        #print(len(self.db))
        for db in self.db:
            db_dir = os.path.join(cw_dir, db.name)
            all_tables = os.listdir(db_dir)
            for tbl in all_tables:
               #print(tbl)
                if os.path.isfile(os.path.join(db_dir, tbl)):
                    with open(os.path.join(db_dir, tbl), 'r') as table_to_read:
                        table_attributes = table_to_read.readline()
                        t = Table(tbl, table_attributes.split('|'))
                        db.add(t)
                        #print(t.print_attributes())

    # this method is responsible for getting the initial command form the user
    # input then returning an index so that execute can call the appropriate method
    def parseCommand(self, command):
        validCommands = ["create", "alter", "drop", "update", "use", "select", "insert", ".exit", ".table", ".database"]
        currentCommand = -1
        if(command[0].lower().rstrip(';') in validCommands):
            currentCommand = validCommands.index(command[0].lower().rstrip(';'))
            #print(currentCommand)
        else:
            print("Syntax Error. Fix statement and try again.")
        return currentCommand

	    
    ## maybe use a list/array for above commands, check if first word in command is in list
    ## if in list then command syntax is off to a good start, otherwise can reject command
    ## need to find a way to return index of matching command if true
    # method responsible for the actual creation of a table
    def createTable(self, new_table, table_attributes, currentDB):
        dbPath = os.path.join(currentDB, new_table)
        if(os.path.isfile(dbPath)):
            print("!Failed to create table {} because it already exists.".format(new_table))
        else:
            with open(dbPath, 'w') as table_file:
                table_file.write(" | ".join(table_attributes))
            db_index = None
            for data_base in self.db:
                if data_base.name == currentDB:
                    db_index = self.db.index(data_base)
                    print(db_index, self.db.index(data_base))
            self.db[db_index].add(Table(new_table, table_attributes))

    def verify_table_attributes(self, attributes):
        valid_types = ['char', 'varchar', 'int', 'float', 'boolean', 'bool', 'text']
        for attribute_pair in attributes:
            attribute_type = attribute_pair.split()[1]
            if attribute_type.find("(") >= 4:
                attribute_type = attribute_type.split("(")[0]
            if attribute_type.lower() not in valid_types:
                print("Syntax error, {} is not an acceptable data type.".format(attribute_type))
                return False
        return True

    # method used to collect input from the user
    def collectInput(self):
        command = input("-->")
        if len(command) <=0:
            command = input("->")
        while command[len(command)-1] != ";":
            command += input("------->")
        return command

    # method responsible for actual create of databases
    def createDB(self, db):
        parent_dir = os.path.abspath(os.getcwd())
        newDBPath = os.path.join(parent_dir, db.rstrip(';'))
        if(os.path.isdir(newDBPath)):
            print("!Failed to create database " + db + " because it already exits.")
        else:
            os.mkdir(newDBPath)
        return Database(db.rstrip(';'))

    # helper method used when all elements of a table are needed
    def select_all_data(self, command, tbl):
        filePath = os.path.join(os.path.abspath(os.getcwd()),self.cur_db)
        filePath = os.path.join(filePath, tbl)
        if "where" not in command.lower():
            with open(filePath, 'r') as f:
                print(f.read())
        else:
            print("can't handle conditionals yet.... please implement. ")

    # helper method for parsing the input command further for easy access to the
    # table attributes upon table creation
    def getTableAttributes(self, command):
        attributes = command[command.find("(")+1:command.find(");")]
        #for x in attributes.split(','):
        #    print(x)
        return attributes.split(",")

def main():
    dbms = DatabaseManagementSystem()
    print("Welcome to sleerDB!:\n")
    while True:
        command = dbms.collectInput()
        commandSwitch = dbms.parseCommand(command.split())
        #print(commandSwitch)
        if commandSwitch >= 0:
            dbms.execute(commandSwitch, command)
main()
