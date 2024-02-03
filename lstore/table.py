from lstore.index import Index
from time import time
from page import Page_Range, Base_Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid:int, key:int, columns:tuple)->None:
        self.rid = rid
        self.key = key
        self.columns = columns

class Page_Range:
    def __init__(self, num_columns:int)->None:
        self.base_pages = [Base_Page(num_columns=num_columns)] * NUM_BASE_PAGES

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
        #CREATE THE PAGE DIRECTORY with SIZE BASED ON THE num_columns 
        self.page_directory = [Page_Range(num_columns=num_columns)]
        self.num_base_records = 0
        self.rid = 0 # increase only when a record is added  
        self.tid = 0 # decrease once a record is added or updated 



    def __merge(self):
        print("merge is happening")
        pass


    def get_rid_value(self):
        self.rid += 1
        return self.rid # returns unique new RID for base records

    def get_tid_value(self):
        self.lid -= 1
        return self.lid # returns unique new TID(RIDs for tails records) - easier to identify if their tail records or base records

    def get_schema_encoding(columns):
        schema_encoding = ''
        for item in columns:
            # if value in column is not 'None' add 1
            if item:
                schema_encoding = schema_encoding + '1'
            # else add 0
            else:
                schema_encoding = schema_encoding + '0'
        return int(schema_encoding, 2)

