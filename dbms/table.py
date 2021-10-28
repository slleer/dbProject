# Author: Stephen Leer
# Date 9/29/21
# version 1.2
from typeInterface import *


class Table:
    def __init__(self, name, attributes):
        self.name = name
        self.attributes = []
        for name_type_pair in attributes:
            self.attributes.append(self.get_data_type(name_type_pair))

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name.lower() == other.lower()
        elif isinstance(other, Table):
            return self.name.lower() == other.name.lower()

    def __str__(self):
        return self.name

    # method used on insert command to ensure the data matches the intended column
    def test_table_data(self, data):
        for x in range(len(data)):
            if not self.attributes[x].check_type(data[x]):
                print("Syntax error, {0} is not of type {1}.".format(data[x], self.attributes[x].attribute_type))
                return False
        return True

    # method used to print the table attributes. Though it is not actually called in the dbms
    def print_attributes(self):
        printable_attributes = []
        for attribute in self.attributes:
            printable_attributes.append(str(attribute))
        print(" | ".join(printable_attributes))

    # helper method used when alter is called to add the new attribute to the table object
    def append_attribute(self, attribute):
        self.attributes.append(self.get_data_type(attribute))

    # helper method used to remove an attribute from a table object, though not used.
    def remove_attribute(self, attribute_name):
        for attribute in self.attributes:
            if attribute_name == attribute.name:
                attribute_index = self.attributes.index(attribute)
                self.attributes.pop(attribute_index)

    # helper method used to ensure a condition is met for the select, update, and delete commands.
    def check_condition(self, index, col_val, condition, argument):
        return self.attributes[index].condition_check(col_val, condition, argument)

    # helper method used for returning a default value when alter table adds a new column, this is
    # only called if the table being altered already has data inserted
    def get_default_value(self):
        return str(self.attributes[len(self.attributes)-1].get_default_value())

    # helper method used to create the necessary typeInterface object (object representing the column
    # types) for a table on table creation, alteration.
    def get_data_type(self, attribute):
        temp_name, temp_type = attribute.split()
        type_value = -1
        if temp_type.find('(') >= 0:
            type_value = int(temp_type[temp_type.find('(') + 1:temp_type.find(')')])
            temp_type = temp_type.split("(")[0]
        if temp_type.lower() == "varchar":
            return VarcharType(temp_name, type_value)
        elif temp_type.lower() == "char":
            return CharType(temp_name.rjust(type_value-len(temp_name)), type_value)
        elif temp_type.lower() == "int":
            return IntegerType(temp_name)
        elif temp_type.lower() == "float":
            return FloatType(temp_name)
        elif temp_type.lower() == "bool" or temp_type.lower() == "boolean":
            return BooleanType(temp_name)
        elif temp_type.lower() == "text":
            return TextType(temp_name, type_value)
        else:
            print("Syntax error, attribute {0} of type {1} is not supported type. ()()()()()()".format(temp_name, temp_type))

        
            
    
