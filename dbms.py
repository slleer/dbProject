import os
import sys
from database import Database

class DatabaseManagementSystem:


	def __inti__(self):
		self.db = []
		self.cur_db = ''
                validCommands = ["create", "alter", "drop", "select", "update", "use", ".exit"]
                validStuctures = ["database", "table"]
                executeCommand = {0: createCommand,
                                  1: alterCommand,
                                  2: dropCommand,
                                  3: updateCommand,
                                  4: useCommand,
                                  5: selectCommand,
                                  6: exitCommand}
		initializeDatabase()

	def selectCommand(self, command):
                file_path = os.path.join(os.path.abspath(os.getcwd()), this.cur_dir)
                if command.split()[1] == '*':
                        table_to_read = command.split()[3]
                        if os.path.isfile(os.path.join(file_path, table_to_read)):
                                selectData(table_to_read)
                        else:
                                print("!Failed to query table", table_to_read, "because it does not exist", sep=' ')
                                
	def alterCommand(self, command):
                if this.cur_dir == '':
                        print("No DATABASE selected, please select DATABASE with USE command and try agian")
                elif command.split()[1].lower() == "table":
                        table_to_alter = command.split()[2]
                        altereration = command.split()[3]
                        if alteration.lower() == 'add':
                                file_path = os.path.join(os.path.abspath(os.getcwd()), this.cur_dir)
                                if os.path.isfile(os.path.join(file_path, table_to_alter)):
                                        table = os.path.join(file_path, table_to_alter)
                                        with open(table, 'a') as file:
                                                for alt in range(4, len(command.split())):
                                                        file.write(', ')
                                                        file.write(command.split()[alt]
                                else:
                                        print("!Failed to alter table", table_to_alter, "because it does not exist.", sep=' ')
                else:
                        print("Syntax error, please review statement and try again")

	def exitCommand(self, command):
                sys.exit()

	def dropCommand(self, command):
                if len(command) == 3:
                        structureType = command.split()[1]
                        structureName = command.split()[2]
                        cw_dir = os.path.abspath(os.getcwd())
                        if structureType.lower() == "database":
                                if os.path.isdir(os.path.join(cw_dir, structureName)):
                                        os.rmdir(os.path.join(cw_dir, structureName))
                                        if structureName in self.db:
                                                self.db.remove(structureName)
                                        if self.cur_db == structureName:
                                                self.cur_db = ''
                                else:
                                        print("!Failed ot delete DATABASE", structureName, "because it does not exist", sep=" ")
                        elif structurType.lower() == "table":
                                tbl_path = os.path.join(cw_dir, self.cur_db)
                                if os.path.isfile(os.path.join(tbl_path, structureName)):
                                        os.remove(os.path.join(tbl_path, structureName))
                                        index = self.db.index(self.cur_db)
                                        self.db[index].remove(structureName)
                                else:
                                        print("!Failed to delete", structureName, "becasue it does not exist.", sep=" ")
                        else:
                                print("Syntax error, please review statement and try again.")
                else:
                        print("Syntax error, please review statement and try again.")

	def useCommand(self, command):
		cw_dir = os.path.abspath(os.getcwd())
		if len(command) == 2:
                        db_to_use = command.split()[1]
			if os.path.isdir(os.path.join(cw_dir, db_to_use)):
				self.cur_db = db_to_use
			else:
                                print("Syntax error,", db_to_use, "is not valid DATABASE", sep=" ")
                else:
                        print("Syntax error, please review statement and try again")


	def createCommand(self, command):
                structureType = command.split()[1] # determine if table or database
                structureName = command.split()[2] # get name of table or database
		if structureType.lower() == "database":
			new_db = createDB(structureName)
			self.db.append(new_db)
		elif structureType.lower() == "table":
			if not self.cur_db:
				print("No DATABASE selected, select DATABASE with USE command and try again")
			else:
				createTable(structureName, getTableAttributes(command), self.cur_db)
                else:
                        print("Syntax error, please review statement and try again")

	def initializeDatabase(self):
		cw_dir = os.path.abspath(os.getcwd())
		allDBs = os.listdir(cw_dir)
		for db in addDBs:
			if os.path.isdir(os.path.join(cw_dir, db)):
				self.db.append(db)
				# going to put a statement here for opening each directory and adding tables but 
				# it might be better to put that in a differnt method that loops through all the
				# databases added here and then adds the tables. 
				
			
				

	def parseCommand(command):
                currentCommand = -1
                if(command[0].lower() in self.validCommands):
                        currenrtCommand = self.validCommands.index(command[0])
                else:
                        print("Syntax Error. Fix statement and try again.")
                return currentCommand

	    
	    ## maybe use a list/array for above commands, check if first word in command is in list
	    ## if in list then command syntax is off to a good start, otherwise can reject command 
	    ## need to find a way to return index of matching command if true

	def createTable(new_table, table_attributes, currentDB):
                print(currentDB)
                dbPath = os.path.join(currentDB, new_table)
                if(os.path.isfile(dbPath)):
                        print("!Failed to create table " + new_table + " because it already exists.")
                else:
                        print("Table " + new_table + " created.")
		
                        try:
                                table_file = open(dbPath, "w")
				try:
                                        table_file.write(table_attributes)
				except:
                                        print("Error writing table to database.")
				finally:
                                        table_file.close()
			except:
				print("Error CREATING table ", new_table, ".")
		    
	def collectInput():
                command = input()
                while(command[len(command)-1] != ";"):
                        command += input("---->")
                return = command

	def createDB(db):
                parent_dir = os.path.abspath(os.getcwd())
                newDBPath = os.path.join(parent_dir, db)
                if(os.path.isdir(newDBPath)):
                        print("!Failed to create database " + db + " because it already exits.")
                else:
                	os.mkdir(newDBPath)
                return Database(db)


	def selectData(self, tbl):
                filePath = os.path.join(os.path.abspath(os.getcwd()),self.cur_dir)
                filePath = os.path.join(filePath, tbl)
                f = open(filePath, "r")
                print(f.read())
                f.close()
	    

	def getTableAttributes(self):
            attributes = self.command[self.command.find("(")+1:self.command.find(");")]
            return attributes.split(",")

while True:
        command = collectInput()
        commandSwitch = parseCommand(command.split())
        if commandSwitch >= 0:
                executeCommand[commandSwitch](command)
