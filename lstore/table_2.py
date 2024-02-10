from lstore.index import Index
from lstore.config import *
from time import time
from page import Page_Range, Base_Page

class Page_Range:
    def __init__(self, num_columns:int)->None:
        self.base_pages = [Base_Page(num_columns=num_columns)] * NUM_BASE_PAGES
        self.tid = 0

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

    """
    :param columns: list        # being list of the columns being passed in when wanting to update 
    
    ex: m1_test.py (line 53) - updated_columns = [None, None, None, None, None]

    - if updated_columns = [None, 5, None, None, None] - then schema encoding would be 01000

    - then we can update the schema encoding by ORing old schema encoding with schema encoding

    - let's say x = 01000, the first encodings, then we update the schema encoding, we'll do 
    - new_schema_encoding = x | get_schema_encoding(columns) - columns being another updates list  [None, None, 10, None, None].
    - This would return 01100, and this would be the updated schema encoding that will be passed with our insert_new_record() function,
    - with our tail record and also passed when updating schema encoding for base record

    """
    def get_schema_encoding(columns):
            schema_encoding = ''
            for item in columns:
                # if value in column is not 'None' add 1
                if item:
                    schema_encoding = schema_encoding + '1'
                # else add 0
                else:
                    schema_encoding = schema_encoding + '0'
            
            print(schema_encoding)
            return int(schema_encoding, 2)