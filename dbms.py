# Author: Stephen Leer
# date: 9/29/21
# version: 2.3
import os
import sys
from database import Database

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
                               6: self.exitCommand}
        self.initializeDatabase()
    # unused update command
    def updateCommand(self, command):
        pass
    # execution method, this method calls the appropriate method depending on
    # the index passed in. Command is the command received from user
    def execute(self, index, command):
        #print(self.cur_db)
        (self.executeCommand[index](command))
        
    # method responsible for displaying table contents to user
    def selectCommand(self, command):
        if not self.cur_db:
            print("No DATABASE selected, select DATABASE using USE command and try again")
        else:
            file_path = os.path.join(os.path.abspath(os.getcwd()), self.cur_db)
            if command.split()[1] == '*':
                table_to_read = command.split()[3].rstrip(';')
                if os.path.isfile(os.path.join(file_path, table_to_read)):
                    self.selectData(table_to_read)
                else:
                    print("!Failed to query table", table_to_read, "because it does not exist", sep=' ')
    # method responsible for altering a table                            
    def alterCommand(self, command):
        if self.cur_db == '':
            print("No DATABASE selected, please select DATABASE with USE command and try agian")
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
    def exitCommand(self, command):
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
                    if structureName in self.db:
                        self.db.remove(structureName)
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
        structureType = command.split()[1] # determine if table or database
        structureName = command.split()[2] # get name of table or database
        if structureType.lower() == "database":
            new_db = self.createDB(structureName)
            self.db.append(new_db)
        elif structureType.lower() == "table":
            if not self.cur_db:
                print("No DATABASE selected, select DATABASE with USE command and try again")
            else:
                self.createTable(structureName, self.getTableAttributes(command), self.cur_db)
        else:
            print("Syntax error, please review statement and try again")

    # method responsible for add existing databases and tables to the system
    # this method is still under development
    def initializeDatabase(self):
        cw_dir = os.path.abspath(os.getcwd())
        allDBs = os.listdir(cw_dir)
        for db in allDBs:
            if os.path.isdir(os.path.join(cw_dir, db)):
                self.db.append(db)
                # going to put a statement here for opening each directory and adding tables but
                # it might be better to put that in a differnt method that loops through all the
                # databases added here and then adds the tables.
    # this method is responsible for getting the initial command form the user
    # input then returing an index so that execute can call the appropriate method
    def parseCommand(self, command):
        validCommands = ["create", "alter", "drop", "update", "use", "select", ".exit"]
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
    # method repsonsible for the actual creation of a table
    def createTable(self, new_table, table_attributes, currentDB):
        dbPath = os.path.join(currentDB, new_table)
        if(os.path.isfile(dbPath)):
            print("!Failed to create table " + new_table + " because it already exists.")
        else:
            with open(dbPath, 'w') as table_file:
                for x in range(len(table_attributes)):
                    table_file.write(table_attributes[x])
                    if x < len(table_attributes):
                        table_file.write(',')
    # method used to collect input from the user
    def collectInput(self):
        command = input()
        while(command[len(command)-1] != ";"):
            command += input("---->")
        return command
    # method responsible for actual create of databases
    def createDB(self, db):
        parent_dir = os.path.abspath(os.getcwd())
        newDBPath = os.path.join(parent_dir, db.rstrip(';'))
        if(os.path.isdir(newDBPath)):
            print("!Failed to create database " + db + " because it already exits.")
        else:
            os.mkdir(newDBPath)
        return Database(db)
    # helper method used when all elements of a table are needed
    def selectData(self, tbl):
        filePath = os.path.join(os.path.abspath(os.getcwd()),self.cur_db)
        filePath = os.path.join(filePath, tbl)
        with open(filePath, 'r') as f:
            print(f.read())
    # helper method for parsing the input command further for easy access to the
    # table attributes upon table creation
    def getTableAttributes(self, command):
        attributes = command[command.find("(")+1:command.find(");")]
        for x in attributes.split(','):
            print(x)
        return attributes.split(",")

def main():
    dbms = DatabaseManagementSystem()
    while True:
        command = dbms.collectInput()
        commandSwitch = dbms.parseCommand(command.split())
        #print(commandSwitch)
        if commandSwitch >= 0:
            dbms.execute(commandSwitch, command)
main()
