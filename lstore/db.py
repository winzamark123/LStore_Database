from lstore.table import Table

import os 
class Database():

    def __init__(self):
        self.tables = {}
        pass

    def open(self, path):
        """
        Takes in a path from the root of the directory and opens the database at that location
        """

        if os.path.isdir(path):
            self.root_name = path





        pass
    
    def close(self):
        pass
    
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name:str, num_columns:int, key_index:int)->Table:
        if name in self.tables:
            print("Table already exists")
            raise ValueError(f"Table {name} already exists.")
        
        table = Table(name, num_columns, key_index)

        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name:str)->None:
        if name in self.tables[name]:
            del self.tables[name]
        else:
            print("Table does not exist")
            raise ValueError(f"Table {name} does not exist.")
        

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name:str)->Table:
        if name in self.tables:
            return self.tables[name]
        else:
            return None
