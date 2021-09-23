import os

def parseCommand(command):
    validCommands = ["create", "alter", "update", "use", "drop", "select", ".exit"]
    currentCommand = -1
    if(command[0] in validCommands):
        currenrtCommand = validCommands.index(command[0])
    
    ## maybe use a list/array for above commands, check if first word in command is in list
    ## if in list then command syntax is off to a good start, otherwise can reject command 
    ## need to find a way to return index of matching command if true

def createTable(new_table, table_attributes, currentDB):
    new_table += ".txt"
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
            print("Error CREATING table" + new_table + ".")
            
def collectInput():
    command = input()
    while(command[len(command)-1] != ";"):
        command += input("---->")
    return command

def createDB(db):
    parent_dir = os.path.abspath(os.getcwd())
    newDBPath = os.path.join(parent_dir, db)
    currentDB = newDBPath
    if(os.path.isdir(newDBPath)):
        print("!Failed to create database " + db + " because it already exits.")
    else:
        os.mkdir(newDBPath)
    return currentDB


def selectData(a, b):
    b += ".txt"
    filePath = os.path.join(a,b)
    f = open(filePath, "r")
    print(f.read())
    f.close()
    

def getTableAttributes(command):
    attributes = command[command.find("(")+1:command.find(");")]
    return attributes


currentDB = ""
newDB = "dbMgnt0"
newDB = createDB(newDB)
atts = "(bort varchar(23), trob int);"
createTable("x", atts, newDB)
a = "create table jack121 (cart varchar(12), time int(10));"
b = a.split()
newTbl = b[2]
attributes = ""
c = 3
while(c != (len(b))):
    attributes += b[c]
    c += 1
createTable(newTbl, attributes, newDB)
createTable(newTbl, attributes, newDB)
selectData(newDB, newTbl)
