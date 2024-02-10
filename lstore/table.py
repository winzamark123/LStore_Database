from lstore.index import Index
from lstore.config import *
from lstore.page import Base_Page
from time import time

class Page_Range:
    def __init__(self, num_columns:int, entry_sizes:list, key_column:int)->None:
        self.num_columns = num_columns
        self.entry_size_for_columns = entry_sizes
        self.key_column = key_column
        self.num_base_records = 0
        self.tid = 0
        
        self.base_pages = [Base_Page(num_columns, entry_sizes, key_column)] * NUM_BASE_PAGES

    def has_capacity(self)-> bool:
        if self.num_base_records < RECORDS_PER_PAGE * NUM_BASE_PAGES:
            return True
        
        return False

    def insert_base_page(self)->None:
        self.base_pages.append(Base_Page(self.num_columns, self.entry_size_for_columns, self.key_column, False))

    def inc_tid(self):
        self.tid -= 1
        return self.tid # returns unique new TID(RIDs for tails records) - easier to identify if they're tail records or base records



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
        self.index = Index(num_columns)

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

        #CREATE THE PAGE DIRECTORY with SIZE BASED ON THE num_columns 
        self.page_directory = [Page_Range(num_columns, self.entry_size_for_columns, self.key_column)]
        # adds integers of 8 to list depending on how many columns are asked from table
        for i in range(num_columns): 
            self.entry_size_for_columns.append(8)


    def get_list_of_addresses(self, rids):
        addreses = []

        for rid in rids:
            page_range_num = rid // (RECORDS_PER_PAGE * NUM_BASE_PAGES)
            base_page_num = (rid // RECORDS_PER_PAGE) % NUM_BASE_PAGES
            addreses.append((page_range_num, base_page_num))
        #return page_range_num, base_page_num
        return addreses

    def __merge(self):
        print("merge is happening")
        pass

    def inc_rid(self):
        self.rid += 1
        return self.rid # returns unique new RID for base records

