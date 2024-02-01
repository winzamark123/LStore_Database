from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid:int, key:int, columns:tuple)->None:
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    
    def __init__(self, name:str, num_columns:int, key:int)->None:
        self.name = name
        self.num_columns = num_columns
        self.key = key
        self.index = Index(self)
        self.page_directory = dict()
        self.num_base_records = 0


    def __merge(self):
        print("merge is happening")
        pass
 
