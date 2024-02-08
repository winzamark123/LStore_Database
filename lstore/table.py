from lstore.index import Index
from lstore.config import *
from time import time
from page import Page_Range, Base_Page

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

        # number of base records 
        self.num_base_records = 0

        # rid for base records - increase by 1 only when a record is added (for base records)
        self.rid = 0

        # tid (rid) for tail records - decrease by 1 once a record is added or updated (for tails records)
        self.tid = 0

        # primary key column (StudentID)
        self.key_column = META_DATA_NUM_COLUMNS + key

        # list of the size of each physical page in base pages in Bytes - These first 4 sizes are for the meta columns
        self.entry_size_for_columns = [2,8,8]

        # adds integers of 8 to list depending on how many columns are asked from table
        for i in range(num_columns): 
            entry_size_for_columns.append(8)



    def __merge(self):
        print("merge is happening")
        pass


    def get_rid_value(self):
        self.rid += 1
        return self.rid # returns unique new RID for base records

    def get_tid_value(self):
        self.lid -= 1
        return self.lid # returns unique new TID(RIDs for tails records) - easier to identify if they're tail records or base records

