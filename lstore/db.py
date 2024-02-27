from lstore.table import Table
import pickle

import os 
class Database():

    def __init__(self):
        self.table_directory = {}
        self.table_objects = {}
        self.db_name = None
        self.disk_directory = {} #disk[table_name] = Disk()
        self.root_path = None
        pass

    def open(self, db_name:str) -> None:

        """
        Takes in a path from the root of the directory and opens the database at that location
        """
        self.db_name = db_name # Store the db_name
        root_path = os.getcwd() + '/' + db_name # /*/ECS165

        self.root_path = root_path

        if not os.path.exists(root_path):
            os.makedirs(root_path)
            print("Database directory created at:", root_path)

        #TODO: Load all the info from DB
        
    def close(self):
        #Saving all the Tables in the Database to disk
        table_directory_path = os.path.join(self.root_path, 'table_directory.pkl')
        table_directory_file = open(table_directory_path, 'wb')
        pickle.dump(self.table_directory, table_directory_file)
        table_directory_file.close()

        # for table_name, table in self.table_directory.items():
        #     disk = self.disk_directory[table_name]
        #     disk.save_table_metadata(table)

        self.table_directory = {}

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, table_name:str, num_columns:int, key_index:int)->Table:
        if table_name in self.table_directory:
            print("Table already exists")
            raise ValueError(f"Table {table_name} already exists.")
        
        new_table = Table(self.db_name, table_name, num_columns, key_index)
        self.table_objects[table_name] = new_table
        #{table_name: Disk()}
        #{table_name: Table()}

        #Table() = meta, page_dir, page_range, base_page, tail_page
        #Disk() = save_table_metadata, load_table_metadata, load_page, save_to_disk_physicalPage

        #Save MetaData of Table 
        self.table_directory[table_name] = {
            "table_name": table_name,
            "table_path": os.path.join(self.root_path, table_name),
            "num_columns": num_columns,
            "key_index": key_index
        }

        print(f"Table {table_name} created")

        return new_table

    """
    # Deletes the specified table
    """
    def drop_table(self, table_name:str)->None:
        if table_name in self.tables[table_name]:
            del self.tables[table_name]
            del self.disk_directory[table_name]
            
        else:
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
