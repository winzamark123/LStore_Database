import os
from threading import RLock

from lstore.bufferpool import BUFFERPOOL
from lstore.disk import Disk
from lstore.table import Table

class Database():

    def __init__(self)->None:
        self.tables:dict[str,Table] = None
        self.db_path = ""

        self.lock:RLock = RLock()

    def __get_tables(self)->tuple[list[str],int]:
        table_dirs = Disk.list_directories_in_path(self.db_path)
        return (table_dirs, len(table_dirs))

    def __load_tables(self)->None:
        with self.lock:
            table_paths, _ = self.__get_tables()
            for table_path in table_paths:
                table_name = os.path.basename(table_path)
                metadata = Disk.read_from_path_metadata(table_path)
                self.tables[table_name] = Table(
                    metadata["table_path"],
                    metadata["num_columns"],
                    metadata["key_index"],
                    metadata["num_records"]
                )

    def open(self, path:str)->None:
        self.tables = dict()
        self.db_path = path
        try:
            Disk.create_path_directory(path)
        except FileExistsError:
            print(f"Database at path {path} already exists.")
            self.__load_tables()
        else:
            print(f"Database at path {path} created.")

    def close(self):
        # delete tables (causes cascade of deletes)
        del self.tables
        self.tables = None

    def create_table(self, name:str, num_columns:int, key_index:int)->Table:
        """
        Create new table.

        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """
        # create table directory
        table_path = os.path.join(self.db_path, name)
        if os.path.exists(table_path): raise FileExistsError
        os.mkdir(table_path)
        
        # create table object and its metadata
        metadata = {
            "table_path": table_path,
            "num_columns": num_columns,
            "key_index": key_index,
            "num_records": 0,
        }
        Disk.write_to_path_metadata(table_path, metadata)
        table = Table(table_path, num_columns, key_index, 0)
        return table

    def drop_table(self, name:str)->None:
        """
        Delete specified table.
        """
        del self.tables[name]

    def get_table(self, name:str)->Table:
        """
        Return table with passed name.
        """
        return self.tables[name]
