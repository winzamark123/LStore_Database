from lstore.index import Index
from lstore.config import *
from time import time
from lstore.page_range import Page_Range

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns
    :param key: int             #Index of table key in columns
    """
    
    def __init__(self, name:str, num_columns:int, key_index:int)->None:
        self.name = name
        self.num_columns = num_columns
        self.key_column = META_DATA_NUM_COLUMNS + key_index
        self.deleted_rids = []
        self.index = Index(num_columns, ORDER_CHOICE)

        # number of base records 
        self.num_base_records = 0

        # rid for base records - increase by 1 only when a record is added (for base records)
        self.rid = 0

        # tid (rid) for tail records - decrease by 1 once a record is added or updated (for tails records)
        self.tid = 0

        self.entry_size_for_columns = [2,8,8]
        for i in range(num_columns): 
            self.entry_size_for_columns.append(COLUMN_SIZE)

        #CREATE THE PAGE DIRECTORY with SIZE BASED ON THE num_columns 
        self.page_directory = [Page_Range(num_columns, self.entry_size_for_columns, self.key_column)]

        print("Table created: ", self.name)
        print("Number of columns: ", self.num_columns)
        print("Key column: ", self.key_column)
        

    def get_list_of_addresses(self, rids)-> list:
        addreses = []

        for rid in rids:
            page_range_num = rid // (RECORDS_PER_PAGE * NUM_BASE_PAGES)
            base_page_num = (rid // RECORDS_PER_PAGE) % NUM_BASE_PAGES
            addreses.append((page_range_num, base_page_num))
        #return page_range_num, base_page_num
        return addreses

    def inc_rid(self)-> int:
        self.rid += 1
        return self.rid # returns unique new RID for base records

    def insert_page_range(self)-> bool:
        if not self.page_directory[-1].has_capacity():
            self.page_directory.append(Page_Range(self.num_columns, self.entry_size_for_columns, self.key_column))
            print("Function: insert_page_range(), Total page ranges: ", len(self.page_directory))
            return True
        return False 
