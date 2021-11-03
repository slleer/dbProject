from abc import ABC, abstractmethod
import os
from database import Database
from dbms import get_indices_with_match, condition_met



class ISelection(ABC):
    @abstractmethod
    def select_data(self, command, db):
        pass


class BasicSelection(ISelection):
    def select_data(self, command, db):
        try:
            file_path = os.path.join(os.path.abspath(os.getcwd()), db.name)
            table_to_read = command.split()[command.lower().split().index("from") + 1].rstrip(';')
            if os.path.isfile(os.path.join(file_path, table_to_read.lower())):
                file_path = os.path.join(file_path, table_to_read.lower())
                conditionals = []
                table_obj = None
                if table_to_read in db.table:
                    table_obj = db.table[db.table.index(table_to_read)]
                else:
                    print(f"Table {table_to_read} is not a valid table.")
                table_columns = []
                if command.split()[1] == "*":
                    for attribute_index in range(len(table_obj.attributes)):
                        table_columns.append(attribute_index)
                else:
                    table_columns = get_indices_with_match(
                        command.split()[1:command.lower().split().index("from")], table_obj)
                if "where" in command.lower():
                    for conditions in command.split()[command.lower().split().index("where") + 1:]:
                        conditionals.append(conditions.rstrip(";"))

                with open(file_path, 'r') as file_to_read:
                    lines = file_to_read.readlines()
                    first_line = True
                    for line in lines:
                        if not bool(conditionals) or first_line or \
                                condition_met(line.split(" | "), conditionals, table_obj):
                            first_line = False
                            data_pieces = line.split("|")
                            print_str = []
                            for i in table_columns:
                                print_str.append(data_pieces[i].strip())
                            print(" | ".join(print_str))
            else:
                print("!Failed to query table {} because it does not exist".format(table_to_read))
        except:
            print("Syntax error, failed to select from table.")


class InnerJoinSelection(ISelection):
    def select_data(self, command, db):
        try:
            file_path = os.path.join(os.path.abspath(os.getcwd()), db.name)
            if 'join' in command.lower():
                tables = command.split()[command.lower().split().index("from")+1: command.lower().split().index("on")]
                if tables[2].lower() == 'inner' and tables[3].lower() == 'join' and len(tables) == 6:
                    table_str_a = tables[0].lower()
                    table_a_var = tables[1]
                    table_str_b = tables[4].lower()
                    table_b_var = tables[5]
                elif tables[2].lower() == 'join' and len(tables) == 5:
                    table_str_a = tables[0].lower()
                    table_a_var = tables[1]
                    table_str_b = tables[3].lower()
                    table_b_var = tables[4]
                else:
                    print("Syntax error, please review statement and try again.")
                    return
            else:
                command_list = command.lower().split()
                tables = command.split()[command_list.index("from") + 1: command_list.index("where")]
                if len(tables) == 4:
                    table_str_a = tables[0].lower()
                    table_a_var = tables[1].rstrip(',')
                    table_str_b = tables[2].lower()
                    table_b_var = tables[3]
                else:
                    print(f"Syntax error, comma separated inner join should only have 4 arguments but {len(tables)}"
                          f" given.")
                    return
            if os.path.isfile(os.path.join(file_path, table_str_a)) and\
                    os.path.isfile(os.path.join(file_path, table_str_b)):
                file_path_a = os.path.join(file_path, table_str_a)
                file_path_b = os.path.join(file_path, table_str_b)
                conditionals = []
                table_a = None
                table_b = None
                table_a_index = None
                table_b_index = None
                if table_str_a in db.table and table_str_b in db.table:
                    table_a = db.table[db.table.index(table_str_a)]
                    table_b = db.table[db.table.index(table_str_b)]
                else:
                    print("Syntax error, both tables need to be from same database")
                table_a_columns = []
                table_b_columns = []
                if command.split()[1] == "*":
                    for attribute_index in range(len(table_a.attributes)):
                        table_a_columns.append(attribute_index)
                    for attribute_index in range(len(table_b.attributes)):
                        table_b_columns.append(attribute_index)
                else:
                    table_a_columns = get_indices_with_match(
                        command.split()[1:command.lower().split().index("from")], table_a)
                    table_b_columns = get_indices_with_match(
                        command.split()[1:command.lower().split().index("from")], table_b)
                if "on" in command.lower():
                    for conditions in command.split()[command.lower().split().index("on") + 1:]:
                        conditionals.append(conditions.rstrip(";"))
                if "where" in command.lower():
                    for conditions in command.split()[command.lower().split().index("where") + 1:]:
                        conditionals.append(conditions.rstrip(";"))
                if len(conditionals) == 3:
                    for c in conditionals:
                        if c.split('.')[0] == table_a_var:
                            temp = get_indices_with_match(c.split('.'), table_a)
                            if len(temp) == 1:
                                table_a_index = temp[0]
                        elif c.split('.')[0] == table_b_var:
                            temp = get_indices_with_match(c.split('.'), table_b)
                            if len(temp) == 1:
                                table_b_index = temp[0]
                else:
                    print(f"Syntax error, there should be only 3 conditional arguments but {len(conditionals)} given.")
                    return
                with open(file_path_a, 'r') as file_a:
                    first_line_a = file_a.readline()
                    lines_a = file_a.readlines()
                with open(file_path_b,'r') as file_b:
                    first_line_b = file_b.readline()
                    lines_b = file_b.readlines()
                print_str = []
                if not table_a_columns:
                    print_str.append(first_line_a.strip())
                else:
                    temp = first_line_a.split('|')
                    for i in table_a_columns:
                        print_str.append(temp[i].strip())
                if not table_b_columns:
                    print_str.append(first_line_b.strip())
                else:
                    temp = first_line_b.split(' | ')
                    for i in table_b_columns:
                        print_str.append(temp[i].strip())
                print(" | ".join(print_str))
                print_str.clear()
                for line_a in lines_a:
                    for line_b in lines_b:
                        data_a = line_a.split(" | ")
                        data_b = line_b.split(" | ")
                        if table_a.check_condition(
                                table_a_index, data_a[table_a_index], conditionals[1], data_b[table_b_index]):
                            if not table_a_columns:
                                print_str.append(line_a.strip())
                            else:
                                for i in table_a_columns:
                                    print_str.append(data_a[i].strip())
                            if not table_b_columns:
                                print_str.append(line_b.strip())
                            else:
                                for i in table_b_columns:
                                    print_str.append(data_b[i].strip())
                            print(" | ".join(print_str))
                            print_str.clear()
            else:
                print("!Failed to join tables {0}, {1} because one/both do not exist".format(table_str_a, table_str_b))
        except:
            print("Syntax error, failed to join tables.")


class LeftOuterJoinSelection(ISelection):
    def select_data(self, command, db):
        try:
            file_path = os.path.join(os.path.abspath(os.getcwd()), db.name)
            tables = command.split()[command.lower().split().index("from") + 1: command.lower().split().index("on")]
            if tables[2].lower() == 'left' and tables[3].lower() == 'outer' and tables[4].lower() == 'join':
                table_str_a = tables[0].lower()
                table_a_var = tables[1]
                table_str_b = tables[5].lower()
                table_b_var = tables[6]
            else:
                print("Syntax error, please review statement and try again.")
                return
            if os.path.isfile(os.path.join(file_path, table_str_a)) and \
                    os.path.isfile(os.path.join(file_path, table_str_b)):
                file_path_a = os.path.join(file_path, table_str_a)
                file_path_b = os.path.join(file_path, table_str_b)
                conditionals = []
                table_a = None
                table_b = None
                table_a_index = None
                table_b_index = None
                if table_str_a in db.table and table_str_b in db.table:
                    table_a = db.table[db.table.index(table_str_a)]
                    table_b = db.table[db.table.index(table_str_b)]
                else:
                    print("Syntax error, both tables need to be from same database")
                table_a_columns = []
                table_b_columns = []
                if command.split()[1] == "*":
                    for attribute_index in range(len(table_a.attributes)):
                        table_a_columns.append(attribute_index)
                    for attribute_index in range(len(table_b.attributes)):
                        table_b_columns.append(attribute_index)
                else:
                    table_a_columns = get_indices_with_match(
                        command.split()[1:command.lower().split().index("from")], table_a)
                    table_b_columns = get_indices_with_match(
                        command.split()[1:command.lower().split().index("from")], table_b)
                for conditions in command.split()[command.lower().split().index("on") + 1:]:
                    conditionals.append(conditions.rstrip(";"))
                if len(conditionals) == 3:
                    for c in conditionals:
                        if c.split('.')[0] == table_a_var:
                            temp = get_indices_with_match(c.split('.'), table_a)
                            if len(temp) == 1:
                                table_a_index = temp[0]
                        elif c.split('.')[0] == table_b_var:
                            temp = get_indices_with_match(c.split('.'), table_b)
                            if len(temp) == 1:
                                table_b_index = temp[0]
                else:
                    print(f"Syntax error, there should be only 3 conditional arguments but {len(conditionals)} given.")
                    return
                with open(file_path_a, 'r') as file_a:
                    first_line_a = file_a.readline()
                    lines_a = file_a.readlines()
                with open(file_path_b, 'r') as file_b:
                    first_line_b = file_b.readline()
                    lines_b = file_b.readlines()
                print_str = []
                if not table_a_columns:
                    print_str.append(first_line_a.strip())
                else:
                    temp = first_line_a.split('|')
                    for i in table_a_columns:
                        print_str.append(temp[i].strip())
                if not table_b_columns:
                    print_str.append(first_line_b.strip())
                else:
                    temp = first_line_b.split(' | ')
                    for i in table_b_columns:
                        print_str.append(temp[i].strip())
                print(" | ".join(print_str))
                print_str.clear()
                for line_a in lines_a:
                    data_a = line_a.split(" | ")
                    table_b_added = False
                    for line_b in lines_b:
                        data_b = line_b.split(" | ")
                        if table_a.check_condition(
                                table_a_index, data_a[table_a_index], conditionals[1], data_b[table_b_index]):
                            table_b_added = True
                            if not table_a_columns:
                                print_str.append(line_a.strip())
                            else:
                                for i in table_a_columns:
                                    print_str.append(data_a[i].strip())
                            if not table_b_columns:
                                print_str.append(line_b.strip())
                            else:
                                for i in table_b_columns:
                                    print_str.append(data_b[i].strip())
                            print(" | ".join(print_str))
                            print_str.clear()
                    if not table_b_added:
                        if not table_a_columns:
                            print_str.append(line_a.strip())
                        else:
                            for i in table_a_columns:
                                print_str.append(data_a[i].strip())
                        if not table_b_columns:
                            for ta in table_b.attributes():
                                print_str.append('')
                        else:
                            for i in table_b_columns:
                                print_str.append('')
                        print(" | ".join(print_str))
                    print_str.clear()
            else:
                print("!Failed to join tables {0}, {1} because one/both do not exist".format(table_str_a, table_str_b))
        except:
            print("Syntax error, failed to join tables.")
