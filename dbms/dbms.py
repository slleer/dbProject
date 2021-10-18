# Author: Stephen Leer
# date: 9/29/21
# version: 2.3
import os
import sys
import shutil
from database import Database
from table import Table


class DatabaseManagementSystem:
    # constructor
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
                               7: self.pipeCommand,
                               8: self.deleteCommand,
                               9: self.exitCommand,
                               10: self.listTables,
                               11: self.listDatabases,
                               12: self.manualCommand}
        self.initializeDatabase()
    def manualCommand(self):
        self.print_help_manual()

    def listTables(self):
        if self.cur_db == "":
            print("No database selected, select database with USE command and try again.")
        else:
            for db in self.db:
                if db.name == self.cur_db:
                    for tbl in db.table:
                        print(tbl.name)
                    return

    def listDatabases(self):
        for db in self.db:
            print(db.name)

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
                                    print(" | ".join(table_data), file=table_to_append)
                                    print("1 new record inserted.")
                                    return
            print("!Failed to insert data into {0} because it does not exist.".format(table))
        else:
            print('!Failed to insert data, please review statement and try again.')


    def get_insertion_values(self, command):
        data = command[command.find("(") + 1:command.find(");")]
        command_vals = command[:command.find("(")]
        data = "".join(data.split())
        return command_vals.split(), data.split(',')

    # unused update command
    def updateCommand(self, command):
        cw_dir = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
        table_to_update = command.split()[1]
        table_dir = os.path.join(cw_dir, table_to_update)
        update_attributes = []
        table_obj = None
        table_element = []
        conditionals = []
        try:
            if len(command.split()) >= 10 and os.path.isfile(table_dir) and command.split()[2].lower() == "set":
                for db in self.db:
                    if db.name == self.cur_db:
                        for tbl in db.table:
                            if tbl.name.lower() == table_to_update.lower():
                                table_obj = tbl
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
                        if self.condition_met(lines.split(" | "), conditionals, table_obj):
                            data_pieces = lines.split("|")
                            print_str = []
                            num_tuples_updated += 1
                            update_index = 0
                            for i in range(len(table_obj.attributes)):
                                if table_obj.attributes[i].attribute_name in table_element:
                                    print_str.append(update_attributes[update_index])
                                    update_index += 1
                                else:
                                    print_str.append(data_pieces[i])
                            print(" | ".join(print_str), file=out_file)
                        else:
                            out_file.write(lines)
                    print("{0} record(s) updated".format(num_tuples_updated))
            else:
                print("Syntax error, please review statement and try again.")
        except:
            print("Syntax error, please review alter statement and try again.")

    def deleteCommand(self, command):
        cw_dir = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
        table_obj = None
        conditionals = []
        try:
            table_to_update = command.split()[2]
            table_dir = os.path.join(cw_dir, table_to_update)
            if len(command.split()) >= 7 and os.path.isfile(table_dir) and command.split()[1].lower() == "from":
                for db in self.db:
                    if db.name == self.cur_db:
                        for tbl in db.table:
                            if tbl.name.lower() == table_to_update.lower():
                                table_obj = tbl
                for conditions in command.split()[command.lower().split().index("where") + 1:]:
                    conditionals.append(conditions.rstrip(";"))
                with open(table_dir, "r") as in_file:
                    first_line = in_file.readline()
                    list_data = in_file.readlines()
                with open(table_dir, "w") as out_file:
                    num_tuples_deleted = 0
                    out_file.write(first_line)
                    for lines in list_data:
                        if self.condition_met(lines.split(" | "), conditionals, table_obj):
                            num_tuples_deleted += 1
                        else:
                            out_file.write(lines)
                    print("{0} record(s) deleted".format(num_tuples_deleted))
            else:
                print("Syntax error, please review statement and try again.")
        except:
            print("Syntax error, please review delete statement and try again.")


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
                        temp_line = line
                        while temp_line[len(temp_line.rstrip())-1] != ";":
                            if temp_line[0] == ".":
                                break
                            temp_line += " " + sql_file.readline().strip()
                        command_switch = self.parseCommand(temp_line.split())
                        if command_switch >= 0:
                            self.execute(command_switch, temp_line)

    # execution method, this method calls the appropriate method depending on
    # the index passed in. Command is the command received from user
    
    def execute(self, index, command):
        if index >= len(self.executeCommand)-4:
            self.executeCommand[index]()
        else:
            (self.executeCommand[index](command))
        
    # method responsible for displaying table contents to user
    def selectCommand(self, command):
        if not self.cur_db:
            print("No DATABASE selected, select DATABASE using USE command and try again")
        else:
            try:
                file_path = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
                table_to_read = command.split()[command.lower().split().index("from")+1].rstrip(';')
                if os.path.isfile(os.path.join(file_path, table_to_read)):
                    file_path = os.path.join(file_path, table_to_read)
                    conditionals = []
                    table_obj = None
                    for db in self.db:
                        if db.name == self.cur_db:
                            for tbl in db.table:
                                if tbl.name.lower() == table_to_read.lower():
                                    table_obj = tbl
                    table_columns = []
                    if command.split()[1] == "*":
                        for attribute_index in range(len(table_obj.attributes)):
                            table_columns.append(attribute_index)
                    else:
                        table_columns = self.get_indices_with_match(
                            command.split()[1:command.lower().split().index("from")], table_obj)
                    if "where" in command.lower():
                        for conditions in command.split()[command.lower().split().index("where") + 1:]:
                            conditionals.append(conditions.rstrip(";"))

                    with open(file_path, 'r') as file_to_read:
                        lines = file_to_read.readlines()
                        first_line = True
                        for line in lines:
                            if not bool(conditionals) or first_line or self.condition_met(line.split(" | "),
                                                                                          conditionals, table_obj):
                                first_line = False
                                data_pieces = line.split("|")
                                print_str = []
                                for i in table_columns:
                                    print_str.append(data_pieces[i])
                                print(" | ".join(print_str))
                else:
                    print("!Failed to query table {} because it does not exist".format(table_to_read))
            except:
                print("Syntax error, please review statement and try again.")
                
    def condition_met(self, data, conditions, table):
        if len(conditions) == 3:
            column_index = self.get_indices_with_match(conditions, table)
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
                column_index = self.get_indices_with_match(conditions_list[i], table)
                temp_condition = table.check_condition(column_index[0], data[column_index[0]], conditions_list[i][1], conditions_list[i][2])
                condition_passed = condition_passed and temp_condition
            return condition_passed
        else:
            return False


    def get_indices_with_match(self, columns, table):
        attribute_col_index = []
        index = 0
        for attribute in table.attributes:
            for col in columns:
                if col.rstrip(",") == attribute.attribute_name.lstrip():
                    attribute_col_index.append(index)
            index += 1
        return attribute_col_index

    def get_indices(self, command, lower_bound=None, upper_bound=None):
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
        elif len(command.split()) < 5:
            print("Syntax error, please review statement and try again")
        elif command.split()[1].lower() == "table":
            table_to_alter = command.split()[2].rstrip(';')
            table_obj = None
            for db in self.db:
                if self.cur_db == db.name:
                    for tbl in db.table:
                        if tbl.name == table_to_alter:
                            table_obj = tbl
            alteration = command.split()[3]
            if alteration.lower() == 'add':
                file_path = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
                if os.path.isfile(os.path.join(file_path, table_to_alter)):
                    table = os.path.join(file_path, table_to_alter)
                    attribute = []
                    with open(table, 'a') as f:
                        f.write(" | ")
                        for alt in range(4, len(command.split())):
                            print_str = " " + command.split()[alt].rstrip(";")
                            f.write(print_str)
                            attribute.append(command.split()[alt].rstrip(";"))
                        table_obj.append_attribute(" ".join(attribute))
                    print("Table {0} modified.".format(table_to_alter))

                else:
                    print("!Failed to alter table", table_to_alter, "because it does not exist.", sep=' ')
        else:
            print("Syntax error, please review statement and try again")

    # method responsible for terminating the program
    def exitCommand(self):
        sys.exit()

    #  responsible for droping databases or tables
    def dropCommand(self, command):
        if len(command.split()) == 3:
            structureType = command.split()[1].lower()
            structureName = command.split()[2].rstrip(';')
            cw_dir = os.path.abspath(os.getcwd())
            if structureType == "database":
                if os.path.isdir(os.path.join(cw_dir, structureName)):
                    try:
                        os.rmdir(os.path.join(cw_dir, structureName))
                    except OSError:
                        proceed = input("Database is not empty, delete anyway? y/n")
                        if proceed.lower() == "y":
                            shutil.rmtree(os.path.join(cw_dir, structureName))
                    print("Database {0} deleted.".format(structureName))
                    for db in self.db:
                        if db.name == structureName:
                            self.db.remove(db)
                    if self.cur_db == structureName:
                        self.cur_db = ''
                else:
                    print("!Failed ot delete DATABASE", structureName, "because it does not exist", sep=" ")
            elif structureType.lower() == "table":
                tbl_path = os.path.join(cw_dir, self.cur_db)
                if os.path.isfile(os.path.join(tbl_path, structureName)):
                    os.remove(os.path.join(tbl_path, structureName))
                    print("Table {0} deleted.".format(structureName))
                    for db in self.db:
                        if db.name == self.cur_db:
                            for tbl in db.table:
                                if tbl.name == structureName:
                                    db.table.remove(tbl)
                else:
                    print("!Failed to delete", structureName, "because it does not exist.", sep=" ")
            else:
                print("Syntax error, please review statement and try again. not table or database.")
        else:
            print("Syntax error, please review statement and try again.")

    # method responsible for selecting the database
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
        for db in self.db:
            db_dir = os.path.join(cw_dir, db.name)
            all_tables = os.listdir(db_dir)
            for tbl in all_tables:
                if os.path.isfile(os.path.join(db_dir, tbl)):
                    with open(os.path.join(db_dir, tbl), 'r') as table_to_read:
                        table_attributes = table_to_read.readline()
                        t = Table(tbl, table_attributes.split('|'))
                        db.add(t)

    # this method is responsible for getting the initial command form the user
    # input then returning an index so that execute can call the appropriate method
    def parseCommand(self, command):
        validCommands = ["create", "alter", "drop", "update", "use", "select", "insert", "pipe", "delete",
                         ".exit", ".table", ".database", ".help"]
        currentCommand = -1
        if command[0].lower().rstrip(';') in validCommands:
            currentCommand = validCommands.index(command[0].lower().rstrip(';'))
        else:
            print("Syntax Error. Fix statement and try again.")
        return currentCommand

    # maybe use a list/array for above commands, check if first word in command is in list
    # if in list then command syntax is off to a good start, otherwise can reject command
    # need to find a way to return index of matching command if true
    # method responsible for the actual creation of a table
    def createTable(self, new_table, table_attributes, currentDB):
        dbPath = os.path.join(currentDB, new_table)
        if(os.path.isfile(dbPath)):
            print("!Failed to create table {} because it already exists.".format(new_table))
        else:
            with open(dbPath, 'w') as table_file:
                print(" | ".join(table_attributes), file=table_file)
            db_index = None
            print("Table {0} created.".format(new_table))
            for data_base in self.db:
                if data_base.name == currentDB:
                    db_index = self.db.index(data_base)
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

    # helper method used when all elements of a table are needed
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
    print("\t\t\t\tWelcome to sleerDB!:\nTo see a list of commands and their usage, use .help\nTo import "
          "statements from file use PIPE fileName;\n example: PIPE PA1_test.sql;")
    #temp_line = ""
    #for line in sys.stdin:
    #    if len(line) > 0 and (line[0].isalpha() or line[0] == "."):
    #        temp_line += line.rstrip()
    #        if temp_line[len(temp_line.rstrip()) - 1] != ";":
    #            if temp_line[0] == ".":
    #                command_switch = dbms.parseCommand(temp_line.split())
    #                if command_switch >= 0:
    #                    dbms.execute(command_switch, temp_line)
    #                    temp_line = ""
    #        else:
    #            command_switch = dbms.parseCommand(temp_line.split())
    #            if command_switch >= 0:
    #                dbms.execute(command_switch, temp_line)
    #                temp_line = ""
    while True:
        command = dbms.collectInput()
        commandSwitch = dbms.parseCommand(command.split())
        if commandSwitch >= 0:
            dbms.execute(commandSwitch, command)
main()
