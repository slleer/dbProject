class Table:
    def __init__(self, name, atts):
        self.name = name
        self.attributeNames = []
        self.attributeTypes = []
        self.typeValues = []        
        for x in atts:
            tempName, tempType = x.split()
            self.attributeNames.append(tempName)
            self.attributeTypes.append(tempType)
            if(tempType.find('(') >= 0):
                self.typeValues.append(int(tempType[tempType.find('(')+1:tempType.find(')')]))
            else:
                self.typeValues.append(0)

    def printAttributes(self):
        index = 1
        for x in range(len(self.attributeNames)):
            print(self.attributeNames[x], self.attributeTypes[x], sep=' ', end='')
            if(index < len(self.attributeNames)):
                print(", ", end='')
            else:
                print('\n', end='')

        
            
