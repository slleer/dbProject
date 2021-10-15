from abc import ABC, abstractmethod


class TypeInterface(ABC):

    def __init__(self, attribute_name, attribute_value=0, attribute_type=None):
        self.attribute_name = attribute_name
        self.attribute_value = attribute_value
        self.attribute_type = attribute_type

    @abstractmethod
    def check_type(self, attribute):
        pass

    def __str__(self):
        if self.attribute_value > 0:
            return "{0} {1}({2})".format(self.attribute_name, self.attribute_type, self.attribute_value)
        else:
            return "{0} {1}".format(self.attribute_name, self.attribute_type)


class TextType(TypeInterface):

    def __init__(self, attribute_name, attribute_value=0):
        super().__init__(attribute_name, attribute_value, "text")

    def check_type(self, attribute):
        if type(attribute) != str():
            print("Syntax error, {0} is not of type Text".format(attribute))
            return False

        if self.attribute_value < len(attribute):
            print("Syntax error, {0} is too large for {1} of type Text({2})".format(attribute, self.attribute_name, self.attribute_value))
            return False

        return True


class FloatType(TypeInterface):

    def __init__(self, attribute_name, attribute_value=0):
        super().__init__(attribute_name, attribute_value, "float")

    def condition_check(self, col_val, condition, argument):
        if not self.check_type(argument):
            return False
        if condition == "=":
            return float(argument) == col_val
        elif condition == "<":
            return float(col_val) < float(argument)
        elif condition == ">":
            return float(col_val) > float(argument)
        elif condition == ">=":
            return float(col_val) >= float(argument)
        elif condition == "<=":
            return float(col_val) <= float(argument)
        else:
            return False


    def check_type(self, attribute):
        try:
            float(attribute)
            return True
        except ValueError:
            print("Syntax error {} is not of type Float".format(attribute))
            return False


class CharType(TypeInterface):

    def __init__(self, attribute_name, attribute_value=0):
        super().__init__(attribute_name, attribute_value, "char")

    def condition_check(self, col_val, condition, argument):
        if not self.check_type(argument):
            return False
        if condition == "=":
            return argument == col_val
        else:
            return False

    def check_type(self, attribute):
        if type(attribute) != str():
            print("Syntax error, {0} is not of type Char".format(attribute))
            return False

        if self.attribute_value != len(attribute):
            print("Syntax error, {0} is too large for {1} of type char({2})".format(attribute, self.attribute_name,
                                                                                   self.attribute_value))
            return False

        return True


class VarcharType(TypeInterface):

    def __init__(self, attribute_name, attribute_value=0):
        super().__init__(attribute_name, attribute_value, "varchar")

    def condition_check(self, col_val, condition, argument):
        if not self.check_type(argument):
            return False
        if condition == "=":
            return argument == col_val
        else:
            return False

    def check_type(self, attribute):
        if not isinstance(attribute, str):
            print("Syntax error, {0} is not of type Varchar.".format(attribute))
            return False

        if self.attribute_value <= len(attribute):
            print("Syntax error, {0} is too large for {1} of type varchar({2})".format(attribute, self.attribute_name, self.attribute_value))
            return False

        return True

class IntegerType(TypeInterface):

    def __init__(self, attribute_name, attribute_value=0):
        super().__init__(attribute_name, attribute_value, "int")

    def condition_check(self, col_val, condition, argument):
        if not self.check_type(argument):
            return False
        if condition == "=":
            return int(argument) == col_val
        elif condition == "<":
            return col_val < int(argument)
        elif condition == ">":
            return col_val > int(argument)
        elif condition == ">=":
            return col_val >= int(argument)
        elif condition == "<=":
            return col_val <= int(argument)
        else:
            return False

    def check_type(self, attribute):
        try:
            int(attribute)
            return True
        except ValueError:
            print("Syntax error, {0} is not of type Integer".format(attribute))
            return False

class BooleanType(TypeInterface):

    def __init__(self, attribute_name, attribute_value=0):
        super().__init__(attribute_name, attribute_value, "boolean")

    def condition_check(self, col_val, condition, argument):
        if not self.check_type(argument):
            return False
        if condition == "=":
            return argument.lower() == col_val.lower()
        else:
            return False

    def check_type(self, attribute):
        if attribute.lower() == "true" or attribute.lower() == 'false':
            return True
        else:
            print('Syntax error, {0} is not of type Bool'.format(attribute))
            return False