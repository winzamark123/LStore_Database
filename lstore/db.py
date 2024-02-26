from lstore.table import Table
from lstore.disk import Disk

import os 
class Database():

    def __init__(self):
        self.tables = {}
        self.db_name = None
        self.disks = {}
        pass

    def open(self, db_name:str) -> None:

        """
        Takes in a path from the root of the directory and opens the database at that location
        """
        self.db_name = db_name # Store the db_name
        path_name = os.getcwd() + '/' + db_name # /*/ECS165

        if not os.path.exists(path_name):
            os.makedirs(path_name)
            print("Database directory created at:", path_name)
        
    def close(self):
        for table_name, table in self.tables.items():
            disk = self.disks[table_name]
            disk.save_table_metadata(table)
            base_page_to_write = table.page_directory[0].base_pages[0].physical_pages
            print("Length of base_page_to_write", len(base_page_to_write))
            print("base_page_to_write", base_page_to_write)
            for i in range(table.num_columns):
                    physical_page_to_write = base_page_to_write[i]
                    disk.save_to_disk_physicalPage(0, False, i, physical_page_to_write)
        self.tables = {}

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, table_name:str, num_columns:int, key_index:int)->Table:
        if table_name in self.tables:
            print("Table already exists")
            raise ValueError(f"Table {table_name} already exists.")
        
        new_table = Table(table_name, num_columns, key_index)
        self.tables[table_name] = new_table
        self.disks[table_name] = Disk(db_name=self.db_name, table_name=table_name, num_columns=num_columns)
        print(f"Table {table_name} created")

        return new_table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, table_name:str)->None:
        if table_name in self.tables[table_name]:
            del self.tables[table_name]
            del self.disks[table_name]
            
        else:
            print("Table does not exist")
            raise ValueError(f"Table {table_name} does not exist.")
        

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name:str)->Table:
        if name in self.tables:
            return self.tables[name]
        else:
            print("Table does not exist")
            return None
