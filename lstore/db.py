from lstore.table import Table
from lstore.disk import Disk
import os
from shutil import rmtree

class Database:

    def __init__(self)->None:
        self.db_dir_path:str        = None
        self.tables:dict[str,Table] = None
        self.disk = None

    def open(self, db_dir_path:str)->None:
        """
        Takes in a path from the root of the directory and opens the database at that location.

        If a database is already initialized, raises a ValueError.
        """

        if self.db_dir_path != None:
            raise ValueError

        self.db_dir_path = db_dir_path
        self.disk = Disk(db_dir_path)

        # TODO: load table metadata + data
        self.tables = dict()

    def close(self)->None:
        """
        Saves all tables in database to disk.

        If no database found, raise a ValueError
        """

        if self.db_dir_path == None:
            raise ValueError

        self.db_dir_path = None
        self.disk = None
        for table in self.tables.values():
            table.close()

    def create_table(self, table_name:str, num_columns:int, key_index:int)->None:
        """
        Creates a new table to be inserted into the database.
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """

        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists.")

        table_dir_path = os.path.join(self.db_dir_path, table_name)
        os.makedirs(table_dir_path, exist_ok=False)

        # save metadata of table
        metadata = {
            "table_dir_path": table_dir_path,
            "num_columns": num_columns,
            "key_index": key_index,
            "num_page_ranges": 1
        }
        self.disk.write_metadata_to_disk(table_dir_path, metadata)

        # create table
        self.tables[table_name] = Table(table_dir_path, num_columns, key_index, 1)
        print(f"Table {table_name} created.")

    def drop_table(self, table_name:str)->None:
        """
        Delete the specified table.

        WARNING: This will delete all data associated with the table.
        """

        if not table_name in self.tables[table_name]:
            raise ValueError
        rmtree(os.path.join(self.db_dir_path, table_name))
        del self.tables[table_name]
        print(f"Table {table_name} dropped.")

    def get_table(self, table_name:str)->Table:
        """
        Returns table with the passed name

        If a table isn't found, it raises a ValueError.
        """

        if not table_name in self.tables:
            raise ValueError
        return self.tables[table_name]
