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

    def test_table_data(self, data):
        for x in range(len(data)):
            if not self.attributes[x].check_type(data[x]):
                #print("Syntax error, {0} is not of type {1}.".format(data[x], self.attributes[x].attribute_type))
                return False
        return True

    def print_attributes(self):
        index = 0
        for attribute in self.attributes:
            print(attribute, end='')
            if index < len(self.attributes)-1:
                print(" | ", end='')
                index += 1
            else:
                print('\n', end='')

    def append_attribute(self, attribute):
        self.attributes.append(self.get_data_type(attribute))

    def remove_attribute(self, attribute_name):
        for attribute in self.attributes:
            if attribute_name == attribute.name:
                attribute_index = self.attributes.index(attribute)
                self.attributes.pop(attribute_index)

    def check_condition(self, index, col_val, condition, argument):
        return self.attributes[index].condition_check(col_val, condition, argument)

    def get_data_type(self, attribute):
        temp_name, temp_type = attribute.split()
        type_value = -1
        if temp_type.find('(') >= 0:
            type_value = int(temp_type[temp_type.find('(') + 1:temp_type.find(')')])
            temp_type = temp_type.split("(")[0]
        if temp_type.lower() == "varchar":
            return VarcharType(temp_name.rjust(type_value-len(temp_name)), type_value)
        elif temp_type.lower() == "char":
            return CharType(temp_name, type_value)
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

        
            
    
