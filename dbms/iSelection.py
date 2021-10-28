from abc import ABC, abstractmethod
import os
from database import Database
from dbms import get_indices_with_match, condition_met


class ISelection(ABC):
    @abstractmethod()
    def select_data(self, command, db):
        pass


class BasicSelection(ISelection):
    def select_data(self, command, db):
        if not self.cur_db:
            print("No DATABASE selected, select DATABASE using USE command and try again")
        else:
            try:
                file_path = os.path.join(os.path.abspath(os.getcwd()), db.name)
                table_to_read = command.split()[command.lower().split().index("from") + 1].rstrip(';')
                if os.path.isfile(os.path.join(file_path, table_to_read)):
                    file_path = os.path.join(file_path, table_to_read)
                    conditionals = []
                    table_obj = None
                    for tbl in db.table:
                        if tbl.name.lower() == table_to_read.lower():
                            table_obj = tbl
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
                print("Syntax error, please review statement and try again.")


class InnerJoinSelection(ISelection):
    def select_data(self, command, db):
        pass


class LeftOuterJoinSelection(ISelection):
    def select_data(self, command, db):
        pass
