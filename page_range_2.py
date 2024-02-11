from lstore.config import *
from lstore.page import Base_Page, Tail_Page, Physical_Page
from time import time
from lstore.record import Record
from random import randint

class Page_Range:
    def __init__(self, num_columns:int, entry_sizes:list, key_column:int)->None:
        self.num_columns = num_columns # number of columns in the table
        self.entry_size_for_columns = entry_sizes # list of the size of each physical page in base pages in Bytes [2,8,8] - These first 3 sizes are for the meta columns
        self.key_column = key_column + META_DATA_NUM_COLUMNS # primary key column (StudentID for m1_test)

        # initialize the base pages list with the first base page
        self.base_pages = [Base_Page(self.num_columns, self.entry_size_for_columns, self.key_column)] 

        # initialize the tail pages list with the first tail page
        self.tail_pages = [Tail_Page(self.num_columns, self.entry_size_for_columns, self.key_column)]

        self.tid = 0 # tid (rid) for tail records - decrease by 1 once a record is added or updated (for tails records)
        

    # checks if the page range has capacity for more records
    def has_capacity(self)-> bool:
        if len(self.base_pages) >= NUM_BASE_PAGES:
            # checks if the last base page in the base page list is full
            if not self.base_pages[-1].has_capacity():
                return False 
        return True

    # increment the TID (RID for tails records)
    def inc_tid(self)-> int:
        self.tid -= 1
        return self.tid 

    # insert a new base page to the page range
    def insert_base_page(self)-> bool:
        # checks the last base page in the base page list to see if it's full
        if not self.base_pages[-1].has_capacity(): 
            self.base_pages.append(Base_Page(self.num_columns, self.entry_size_for_columns, self.key_column))
            print("Function: insert_base_page()")
            print("Total base pages in page range: ", len(self.base_pages))
            print("===============================")
            return True
        
        # failed to insert a new base page
        return False

    # (In the works) 
    def update(self, rid:int, columns_of_update:list):
        
        # gets base page number the RID is in 
        base_page_number = self.get_page_number(rid)

        base_page_to_work = self.search_list(self.base_pages, base_page_number)

        # retrieves indirection value for rid
        indirection_base_value = base_page_to_work.check_base_record_indirection(rid)

        # retrieves schema encoding value for rid
        schema_encoding_base_value = base_page_to_work.check_base_record_schema_encoding(rid)

        # new schema encoding based on updates wanted (going into tail record and updating base indirection)
        new_schema_encoding = schema_encoding_base_value | self.get_schema_encoding(columns_of_update)

        # update TID 
        self.tid -= 1

        # checks if the base record indirection is pointing to itself
        if indirection_base_value == rid:
            self.insert_tail_record(self.tid, new_schema_encoding, rid, columns_of_update)
        # else it's pointing to a tail record
        else:
            tail_page_number = self.get_page_number(indirection_base_value)

            tail_page_to_work = self.search_list(self.tail_pages, tail_page_number)

            self.insert_tail_record(self.tid, new_schema_encoding, indirection_base_value, columns_of_update)
        
    def search_list(self, page_list:list, page_number):
        for page in page_list:
            if page.page_number == page_number:
                page_to_work = page
                return page_to_work
            else:
                print(f"RID or LID is not in any of tail pages")

    # inserts tail record into tail page (In the works)
    def insert_tail_record(self, tid:int, schema_encoding:int, indirection:int , columns:list):

        # takes first item in list out since it's just for the key column
        columns.pop(0)

        # make None types equal 0
        for i in range(len(columns)):
            if columns[i] == None:
                columns[i] = 0

        new_tuple = tuple(columns)
        new_tail_record = Record(tid, 0, new_tuple)
        self.tail_pages[-1].insert_new_record(new_tail_record, indirection, schema_encoding, update=True)

    # returns base page number that the rid is on (each base page and tail page have a page number)
    def get_page_number(self,rid:int)->int:
        page_index = rid // (RECORDS_PER_PAGE + 1)
        return page_index + 1
        

    # No sure if this should go in table.py or in page_range.py    
    @staticmethod
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
        
