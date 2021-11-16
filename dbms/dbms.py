# Author: Stephen Leer
# date: 11/02/21
# version: 3.2
import os
import sys
import shutil
import time
from database import Database
from table import Table
from iSelection import *


# helper function that returns the column indices for a given table that match the column names given in a command
# is used in the select and update methods
def get_indices_with_match(columns, table):
    attribute_col_index = []
    index = 0
    for attribute in table.attributes:
        for col in columns:
            if col.rstrip(",") == attribute.attribute_name.lstrip():
                attribute_col_index.append(index)
        index += 1
    return attribute_col_index


# helper function used in select, delete, and update commands to ensure a given condition is met
def condition_met(data, conditions, table):
    if len(data) > 0 and data[0] == '\n':
        return False
    if len(conditions) == 3:
        column_index = get_indices_with_match(conditions, table)
        return table.check_condition(column_index[0], data[column_index[0]], conditions[1], conditions[2])
    elif len(conditions) > 3:
        conditions_list = []
        temp_conditions = conditions.copy()
        for i in range(int(len(temp_conditions)/3)):
            temp = []
            for x in range(3):
                temp.append(temp_conditions[0])
                temp_conditions.pop(0)
            if len(temp_conditions) > 0 and temp_conditions[0].lower() == "and":
                temp_conditions.pop(0)
            conditions_list.append(temp)
        condition_passed = True
        for i in range(len(conditions_list)):
            column_index = get_indices_with_match(conditions_list[i], table)
            temp_condition = table.check_condition(column_index[0], data[column_index[0]], conditions_list[i][1], conditions_list[i][2])
            condition_passed = condition_passed and temp_condition
        return condition_passed
    else:
        return False


class DatabaseManagementSystem:
    # constructor
    def __init__(self):
        self.db = []
        self.cur_db = None
        self.transactionID = None
        self.executeCommand = {0: self.createCommand,
                               1: self.alterCommand,
                               2: self.dropCommand,
                               3: self.updateCommand,
                               4: self.useCommand,
                               5: self.selectCommand,
                               6: self.insertCommand,
                               7: self.pipeCommand,
                               8: self.deleteCommand,
                               9: self.beginTransaction,
                               10: self.commit,
                               11: self.exitCommand,
                               12: self.listTables,
                               13: self.listDatabases,
                               14: self.manualCommand}
        self.initializeDatabase()

    def beginTransaction(self, command):
        if command.split()[1].lower().rstrip(';') == "transaction" and self.cur_db is not None:
            self.transactionID = str(int(float(format(time.time(), '.1f'))*10))
            print("Transaction started.")
        elif self.cur_db is None:
            print("No database selected, please select database before beginning transaction")
        else:
            print("Syntax error, please review statement and try again, {0} is not recognize.".format(command.split()[1]))

    def canModify(self, table):
        cwd = os.path.join(os.path.abspath(os.getcwd()), self.cur_db.name)
        tbl = os.path.join(cwd, table)
        if os.stat(tbl+'log').st_size == 0 and self.transactionID is None:
            return True
        elif os.stat(tbl+'log').st_size == 0 and self.transactionID is not None:
            with open(tbl+'log', 'w') as logFile:
                logFile.write(self.transactionID)
            tbl_temp = tbl+'temp'
            cwd = os.path.join(os.path.abspath(os.getcwd()), self.cur_db.name)
            cwd_temp = os.path.join(cwd, tbl_temp)
            if not os.path.isfile(cwd_temp):
                shutil.copy(os.path.join(cwd, tbl), cwd_temp)
            return True
        else:
            with open(tbl+'log', 'r') as logFile:
                lockingId = logFile.readline()
                if self.transactionID == lockingId:
                    return True
                else:
                    print(f"Error: Table {table} is locked.")
                    return False

    def commit(self):
        modifiedTables = 0
        cwd = os.path.join(os.path.abspath(os.getcwd()), self.cur_db.name)
        tbls = self.cur_db.table
        logCheck = False
        for tbl in tbls:
            if os.stat(os.path.join(cwd, tbl.name+'log')).st_size > 0:
                with open(os.path.join(cwd, tbl.name+'log'), 'r') as log:
                    if self.transactionID == log.readline():
                        logCheck = True
            if logCheck and os.path.isfile(os.path.join(cwd, tbl.name+'temp')):
                shutil.copy(os.path.join(cwd, tbl.name+'temp'), os.path.join(cwd, tbl.name))
                os.remove(os.path.join(cwd, tbl.name+'temp'))
                with open(os.path.join(cwd, tbl.name+'log'), 'w') as log:
                    pass
                modifiedTables += 1
                logCheck = False
        self.transactionID = None
        if modifiedTables > 0:
            print("Transaction committed.")
        else:
            print("Transaction aborted.")


    # prints the user manual for the dbms
    def manualCommand(self):
        self.print_help_manual()

    # will list all tables associated with currently selected database
    def listTables(self):
        if self.cur_db is None:
            print("No database selected, select database with USE command and try again.")
        else:
            for tbl in self.cur_db.table:
                print(tbl.name)
            return

    # will list all databases
    def listDatabases(self):
        for db in self.db:
            print(db.name)

    # inserts data into table
    def insertCommand(self, command):
        commands, table_data = self.get_insertion_values(command)
        if commands[1].lower() == "into" and commands[3].lower() == "values":
            table = commands[2]
            if self.canModify(table):
                if self.transactionID is not None:
                    cw_dir = os.path.join(os.path.abspath(os.getcwd()), self.cur_db.name)
                    cw_dir = os.path.join(cw_dir, table.lower()+'temp')
                else:
                    cw_dir = os.path.join(os.path.abspath(os.getcwd()), self.cur_db.name)
                    cw_dir = os.path.join(cw_dir, table.lower())
            else:
                return
            if table in self.cur_db.table:
                tbl = self.cur_db.table[self.cur_db.table.index(table)]
            if tbl.test_table_data(table_data):
                with open(cw_dir, 'a') as table_to_append:
                    print(" | ".join(table_data), file=table_to_append)
                    print("1 new record inserted.")
                    return
            print("!Failed to insert data into {0} because it does not exist.".format(table))
        else:
            print('!Failed to insert data, please review statement and try again.')

    # helper function that collects the values to be inserted and returns them as a list along with a list containing
    # the keywords and table name
    def get_insertion_values(self, command):
        data = command[command.find("(") + 1:command.find(");")]
        command_vals = command[:command.find("(")]
        data = "".join(data.split())
        return command_vals.split(), data.split(',')

    # update command, uses the table object's list of attributes and their repective condition checking methods
    # to verify conditions to update the given columns.
    def updateCommand(self, command):
        cw_dir = os.path.join(os.path.abspath(os.getcwd()), self.cur_db.name)
        table_to_update = command.split()[1]
        table_dir = os.path.join(cw_dir, table_to_update.lower())
        update_attributes = []
        table_obj = None
        table_element = []
        conditionals = []
        try:
            if self.canModify(table_to_update):
                if self.transactionID is not None:
                    table_dir += 'temp'
            else:
                return
            if len(command.split()) >= 10 and os.path.isfile(table_dir) and command.split()[2].lower() == "set":
                if table_to_update in self.cur_db.table:
                    table_obj = self.cur_db.table[self.cur_db.table.index(table_to_update)]
                else:
                    print(f"Table {table_to_update} is not valid table.")
                    return
                switch = True
                for element in command.split()[3:command.lower().split().index("where")]:
                    if element != "=":
                        if switch:
                            table_element.append(element)
                            switch = not switch
                        else:
                            update_attributes.append(element.rstrip(","))
                            switch = not switch
                for conditions in command.split()[command.lower().split().index("where") + 1:]:
                    conditionals.append(conditions.rstrip(";"))
                if len(table_element) != len(update_attributes):
                    print("Syntax error, please review statement and try again")
                    return
                with open(table_dir, "r") as in_file:
                    first_line = in_file.readline()
                    list_data = in_file.readlines()
                with open(table_dir, "w") as out_file:
                    num_tuples_updated = 0
                    out_file.write(first_line)
                    for lines in list_data:
                        if not lines.split():
                            pass
                        elif condition_met(lines.split(" | "), conditionals, table_obj):
                            data_pieces = lines.split("|")
                            print_str = []
                            num_tuples_updated += 1
                            update_index = 0
                            for i in range(len(table_obj.attributes)):
                                if table_obj.attributes[i].attribute_name in table_element:
                                    print_str.append(update_attributes[update_index])
                                    update_index += 1
                                else:
                                    print_str.append(data_pieces[i].strip())
                            print(" | ".join(print_str), file=out_file)
                        else:
                            out_file.write(lines)
                    print("{0} record(s) updated".format(num_tuples_updated))
            else:
                print("Syntax error, please review statement and try again.")
        except:
            print("Syntax error, please review alter statement and try again.")

    # delete command responsible for deleting tuples from a table
    def deleteCommand(self, command):
        cw_dir = os.path.join(os.path.abspath(os.getcwd()), self.cur_db.name)
        table_obj = None
        conditionals = []
        try:
            table_to_update = command.split()[2]
            table_dir = os.path.join(cw_dir, table_to_update.lower())
            if self.canModify(table_to_update):
                if self.transactionID is not None:
                    table_dir += 'temp'
            else:
                return
            if len(command.split()) >= 7 and os.path.isfile(table_dir) and command.split()[1].lower() == "from":
                if table_to_update in self.cur_db.table:
                    table_obj = self.cur_db.table[self.cur_db.table.index(table_to_update)]
                else:
                    print(f"Table {table_to_update} is not valid table.")
                    return
                for conditions in command.split()[command.lower().split().index("where") + 1:]:
                    conditionals.append(conditions.rstrip(";"))
                with open(table_dir, "r") as in_file:
                    first_line = in_file.readline()
                    list_data = in_file.readlines()
                with open(table_dir, "w") as out_file:
                    num_tuples_deleted = 0
                    out_file.write(first_line)
                    for lines in list_data:
                        if condition_met(lines.split(" | "), conditionals, table_obj):
                            num_tuples_deleted += 1
                        else:
                            out_file.write(lines)
                    print("{0} record(s) deleted".format(num_tuples_deleted))
            else:
                print("Syntax error, please review statement and try again.")
        except:
            print("Syntax error, please review delete statement and try again.")

    # automation function used for executing a series of commands that are stored in a file
    def pipeCommand(self, command):
        if len(command.split()) > 2:
            print("syntax error, pipe command takes 1 argument, more were given.")
        file_path = os.path.join(os.path.abspath(os.getcwd()), command.split()[1].rstrip(";"))
        if os.path.isfile(file_path):
            with open(file_path, 'r') as sql_file:
                temp_line = ""
                while True:
                    line = sql_file.readline().strip()
                    if len(line) > 0 and (line[0].isalpha() or line[0] == "."):
                        temp_line = line.rstrip()
                        while temp_line[len(temp_line.rstrip())-1] != ";":
                            if temp_line[0] == ".":
                                break
                            temp_line += " " + sql_file.readline().strip()
                        command_switch = self.parseCommand(temp_line.split())
                        if command_switch >= 0:
                            self.execute(command_switch, temp_line)
        else:
            print("File not found.")

    # execution method, this method calls the appropriate command method depending on
    # the index passed in. Command is the command received from user
    def execute(self, index, command):
        if index >= len(self.executeCommand)-5:
            self.executeCommand[index]()
        else:
            (self.executeCommand[index](command))
        
    # method responsible for displaying table contents to user, is capable of displaying only table data that
    # meets a specific command.
    def selectCommand(self, command):
        if not self.cur_db:
            print("No DATABASE selected, select DATABASE using USE command and try again")
        else:
            if 'outer' in command.lower() and 'left' in command.lower():
                select = LeftOuterJoinSelection()
            elif 'join' in command.lower() or 'where' in command.lower() and\
                    len(command.split()[command.lower().split().index('from') + 1:command.lower().split().index('where')]) >= 4:
                select = InnerJoinSelection()
            else:
                select = BasicSelection()
            select.select_data(command, self.cur_db)


    # method responsible for altering a table, now adds a default value based on the type being added to all rows that
    # are already inserted when alter is executed.
    def alterCommand(self, command):
        if self.cur_db is None:
            print("No DATABASE selected, please select DATABASE with USE command and try agian")
        elif len(command.split()) < 5:
            print("Syntax error, please review statement and try again")
        elif command.split()[1].lower() == "table":
            table_to_alter = command.split()[2].rstrip(';')
            table_obj = None
            if self.canModify(table_to_alter):
                pass
            else:
                return
            if table_to_alter in self.cur_db.table:
                table_obj = self.cur_db.table[self.cur_db.table.index(table_to_alter)]
            else:
                print(f"Table {table_to_alter} is not valid table.")
                return
            alteration = command.split()[3]
            if alteration.lower() == 'add':
                file_path = os.path.join(os.path.abspath(os.getcwd()), self.cur_db.name)
                if os.path.isfile(os.path.join(file_path, table_to_alter.lower())):
                    table = os.path.join(file_path, table_to_alter.lower())
                    if self.transactionID is not None:
                        table += 'temp'
                    attribute = []
                    with open(table, 'r') as in_file:
                        content = in_file.readlines()
                    first_line = True
                    with open(table, 'w') as f:
                        for line in content:
                            if first_line:
                                first_line = False
                                f.write(line.strip())
                                f.write(" | ")
                                for alt in range(4, len(command.split())):
                                    print_str = " " + command.split()[alt].rstrip(";")
                                    f.write(print_str)
                                    attribute.append(command.split()[alt].rstrip(";"))
                                table_obj.append_attribute(" ".join(attribute))
                                f.write("\n")
                            else:
                                temp_line = line.strip() + " | " + table_obj.get_default_value() + "\n"
                                f.write(temp_line)
                    print("Table {0} modified.".format(table_to_alter))

                else:
                    print("!Failed to alter table", table_to_alter, "because it does not exist.", sep=' ')
        else:
            print("Syntax error, please review statement and try again")

    # method responsible for terminating the program
    def exitCommand(self):
        sys.exit()

    #  method responsible for dropping databases or tables, now includes the ability to drop a database
    #  that isn't empty, deleting its tables as well (this requires an user input to confirm).
    def dropCommand(self, command):
        if len(command.split()) == 3:
            structureType = command.split()[1].lower()
            structureName = command.split()[2].rstrip(';')
            cw_dir = os.path.abspath(os.getcwd())
            if structureType == "database":
                if os.path.isdir(os.path.join(cw_dir, structureName.lower())):
                    try:
                        os.rmdir(os.path.join(cw_dir, structureName.lower()))
                        print("Database {0} deleted.".format(structureName))
                    except OSError:
                        proceed = input("Database is not empty, delete anyway? y/n")
                        if proceed.lower() == "y":
                            shutil.rmtree(os.path.join(cw_dir, structureName.lower()))
                            print("Database {0} deleted.".format(structureName))
                        else:
                            print("Database {0} not deleted.".format(structureName))
                    self.db.remove(structureName)
                    if self.cur_db is not None and self.cur_db.name == structureName:
                        self.cur_db = None
                else:
                    print("!Failed ot delete DATABASE", structureName, "because it does not exist", sep=" ")
            elif structureType.lower() == "table":
                tbl_path = os.path.join(cw_dir, self.cur_db.name)
                if os.path.isfile(os.path.join(tbl_path, structureName.lower())):
                    os.remove(os.path.join(tbl_path, structureName.lower()))
                    os.remove(os.path.join(tbl_path, structureName.lower()+'log'))
                    print("Table {0} deleted.".format(structureName))
                    for tbl in self.cur_db.table:
                        if tbl.name == structureName:
                            self.cur_db.table.remove(tbl)
                else:
                    print("!Failed to delete", structureName, "because it does not exist.", sep=" ")
            else:
                print("Syntax error, please review statement and try again. not table or database.")
        else:
            print("Syntax error, please review statement and try again.")

    # method responsible for selecting the database
    def useCommand(self, command):
        cw_dir = os.path.abspath(os.getcwd())
        if self.transactionID is not None:
            print('Commit current transaction before switching databases.')
            return
        if len(command.split()) == 2:
            db_to_use = command.split()[1].rstrip(';')
            if os.path.isdir(os.path.join(cw_dir, db_to_use.lower())):
                print("USING", db_to_use, sep=' ')
                self.cur_db = self.db[self.db.index(db_to_use)]
            else:
                print("!Failed to USE DATABASE", db_to_use, "because it does not exist", sep=" ")
        else:
            print("Syntax error, please review statement and try again")

    # method responsible for controlling creation of database or table by
    # calling the appropriate method and parsing the command as necessary
    def createCommand(self, command):
        if len(command.split()) < 3:
            print("Syntax error, please review the statement and try again.")
        else:
            structureType = command.split()[1] # determine if table or database
            structureName = command.split()[2] # get name of table or database
            if structureType.lower() == "database":
                self.db.append(self.createDB(structureName.lower()))
            elif structureType.lower() == "table":
                if not self.cur_db:
                    print("No DATABASE selected, select DATABASE with USE command and try again")
                elif self.verify_table_attributes(self.getTableAttributes(command)):
                    if '(' in structureName:
                        temp = structureName.split('(')
                        structureName = temp[0]
                    self.createTable(structureName, self.getTableAttributes(command), self.cur_db.name)
            else:
                print("Syntax error, please review statement and try again.")

    # method responsible for add existing databases and tables to the system when program first runs.
    def initializeDatabase(self):
        cw_dir = os.path.abspath(os.getcwd())
        allDBs = os.listdir(cw_dir)
        for db in allDBs:
            if os.path.isdir(os.path.join(cw_dir, db)) and db[0].isalnum():
                self.db.append(Database(db))
        for db in self.db:
            db_dir = os.path.join(cw_dir, db.name)
            all_tables = os.listdir(db_dir)
            for tbl in all_tables:
                if os.path.isfile(os.path.join(db_dir, tbl)) and 'log' not in tbl and 'temp' not in tbl:
                    with open(os.path.join(db_dir, tbl), 'r') as table_to_read:
                        table_attributes = table_to_read.readline()
                        db.add(Table(tbl, table_attributes.split('|')))

    # this method is responsible for getting the initial command form the user
    # input then returning an index so that execute can call the appropriate method
    def parseCommand(self, command):
        validCommands = ["create", "alter", "drop", "update", "use", "select", "insert", "pipe", "delete", "begin",
                         "commit", ".exit", ".table", ".database", ".help"]
        currentCommand = -1
        if command[0].lower().rstrip(';') in validCommands:
            currentCommand = validCommands.index(command[0].lower().rstrip(';'))
        else:
            print("Syntax Error. Fix statement and try again.")
        return currentCommand

    # method responsible for the actual creation of a table
    def createTable(self, new_table, table_attributes, currentDB):
        dbPath = os.path.join(currentDB, new_table.lower())
        if(os.path.isfile(dbPath)):
            print("!Failed to create table {} because it already exists.".format(new_table))
        else:
            with open(dbPath, 'w') as table_file:
                print(" | ".join(table_attributes), file=table_file)
            db_index = None
            print("Table {0} created.".format(new_table))
            self.db[self.db.index(currentDB)].add(Table(new_table, table_attributes))
            with open(dbPath + 'log', 'w') as logFile:
                pass

    # helper function used to ensure tables are created with only valid attribute types.
    # this prevents issues later on when inserting data, when the data is checked against it's
    # associated type.
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

    # method used to collect input from the user, has been updated to allow for blank lines being entered without
    # breaking the program/method
    def collectInput(self):
        command = input("-->")
        while len(command) <= 0 or command[len(command)-1] != ";":
            if len(command) > 0 and command[0] == ".":
                return command
            command += input("------->")
        return command

    # method responsible for actual create of databases
    def createDB(self, db):
        parent_dir = os.path.abspath(os.getcwd())
        newDBPath = os.path.join(parent_dir, db.rstrip(';'))
        if(os.path.isdir(newDBPath)):
            print("!Failed to create database {0} because it already exits.".format(db.rstrip(";")))
        else:
            os.mkdir(newDBPath)
            print("Database {0} created.".format(db.rstrip(";")))
        return Database(db.rstrip(';'))

    # helper method used to print the user manual, possibly will be updated in the future to print read me as well.
    # might also update this method to take arguments allowing for the display of only specific parts of manual
    def print_help_manual(self):
        filePath = os.path.join(os.path.abspath(os.getcwd()), "sdb_manual")
        with open(filePath, 'r') as f:
            print(f.read())

    # helper method for parsing the input command further for easy access to the
    # table attributes upon table creation
    def getTableAttributes(self, command):
        attributes = command[command.find("(")+1:command.find(");")]
        return attributes.split(",")


def main():
    dbms = DatabaseManagementSystem()
    print("_____Welcome to sleerDB!_____:\nTo see a list of commands and their usage, use .help\nTo import "
          "statements from file use PIPE fileName;\n example: PIPE PA1_test.sql;")
    if len(sys.argv) > 1:
        if os.path.isfile(sys.argv[1]):
            command = "pipe " + sys.argv[1]
            dbms.pipeCommand(command)
    while True:
        command = dbms.collectInput()
        commandSwitch = dbms.parseCommand(command.split())
        if commandSwitch >= 0:
            dbms.execute(commandSwitch, command)


if __name__ == '__main__':
    main()
