from lstore.index import Index
from lstore.config import *
from lstore.page import Base_Page
from time import time

class Page_Range:
    def __init__(self, num_columns:int, entry_sizes:list, key_column:int)->None:
        self.num_columns = num_columns # number of columns in the table
        self.entry_size_for_columns = entry_sizes # list of the size of each physical page in base pages in Bytes [2,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8] - These first 4 sizes are for the meta columns
        self.key_column = key_column # primary key column (StudentID for m1_test)

        # initialize the base pages list with the first base page
        self.base_pages = [Base_Page(self.num_columns, self.entry_size_for_columns, self.key_column, False)] 

        self.tid = 0 # tid (rid) for tail records - decrease by 1 once a record is added or updated (for tails records)
        

    # checks if the page range has capacity for more records
    def has_capacity(self)-> bool:
        # checks if the last base page in the base page list is full
        if len(self.base_pages) >= NUM_BASE_PAGES:
            # checks if the last base page in the base page list is full
            if self.base_pages[-1].page_is_full():
                return False 
        return True

    # increment the TID (RID for tails records)
    def inc_tid(self):
        self.tid -= 1
        return self.tid 

    # insert a new base page to the page range
    def insert_base_page(self)-> bool:
        # checks the last base page in the base page list to see if it's full
        if self.base_pages[-1].page_is_full(): 
            self.base_pages.append(Base_Page(self.num_columns, self.entry_size_for_columns, self.key_column, False))
            print("Function: insert_base_page()")
            print("Total base pages in page range: ", len(self.base_pages))
            print("===============================")
            return True
        
        # failed to insert a new base page
        return False
        
