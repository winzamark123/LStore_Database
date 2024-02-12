from lstore.config import *
from lstore.page import Base_Page, Tail_Page, Physical_Page
from time import time
from lstore.record import Record
from random import randint

class Page_Range:
    def __init__(self, num_columns:int, entry_sizes:list, key_column:int)->None:
        self.num_columns = num_columns # number of columns in the table
        self.entry_size_for_columns = entry_sizes # list of the size of each physical page in base pages in Bytes [2,8,8] - These first 3 sizes are for the meta columns
        self.key_column = key_column # primary key column (StudentID for m1_test)

        # initialize the base pages list with the first base page
        self.base_pages = [Base_Page(self.num_columns, self.entry_size_for_columns, self.key_column)] 

        # initialize the tail pages list with the first tail page
        self.tail_pages = [Tail_Page(self.num_columns, self.entry_size_for_columns, self.key_column)]

        self.deleted_rids = [] # list of deleted rids

        self.tid = 0 # tid (rid) for tail records - decrease by 1 once a record is added or updated (for tails records)
        

    # checks if the page range has capacity for more records
    def has_capacity(self)-> bool:
        if len(self.base_pages) >= NUM_BASE_PAGES:
            # checks if the last base page in the base page list is full
            if not self.base_pages[-1].has_capacity():
                return False 
        return True

    # increment the TID (RID for tails records)
    def inc_tid(self):
        self.tid -= 1

    def delete_base_page(self, rid: int)->None:
        self.deleted_rids.append(rid)

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
    
    # insert a new tail page to the page range
    def insert_tail_pages(self)->None:
        self.tail_pages.append(Tail_Page(self.num_columns, self.entry_size_for_columns, self.key_column))
        print("Function: insert_tail_page()")
        print("Total tail pages in page range: ", len(self.tail_pages))
        print("===============================")

    # updates record
    def update(self, rid:int, columns_of_update:list)->bool:
        
        # gets base page number the RID is in
        base_page_number = self.get_page_number(rid)

        # base page that has RID 
        base_page_to_work = self.search_list(self.base_pages, base_page_number)

        # retrieves indirection value for rid
        indirection_base_value = base_page_to_work.check_base_record_indirection(rid)

        # retrieves schema encoding value for rid
        schema_encoding_base_value = base_page_to_work.check_base_record_schema_encoding(rid)

        print(f'indirection_base_value: {indirection_base_value}')
        print(f'schema_encoding_base_value: {schema_encoding_base_value}')

        # new schema encoding based on updates wanted (going into tail record and updating base indirection)
        new_schema_encoding = schema_encoding_base_value | self.get_schema_encoding(columns_of_update)

        # update TID 
        self.inc_tid()

        # checks if tail page has enough capacity
        if not self.tail_pages[-1].has_capacity(): 
                self.insert_tail_pages() 

        # checks if the base record indirection is pointing to itself
        if indirection_base_value == rid:
            print("inserting tail")
            self.insert_tail_record(self.tid, new_schema_encoding, rid, columns_of_update)
        # else it's pointing to a tail record
        else:
            print("inserting tail that already has tail record")
            # gets tail page number the TID is in
            tail_page_number = self.get_page_number(indirection_base_value)

            print(tail_page_number)

            # tail page TID is in
            tail_page_to_work = self.search_list(self.tail_pages, tail_page_number)

            # retrieves schema encoding value for tid
            schema_encoding_tail_value = tail_page_to_work.check_tail_record_schema_encoding(indirection_base_value)

            print(f'Schema encoding of previous tail page: {schema_encoding_tail_value}')

            # list of the the updated columns in tail record using the schema encoding
            list_of_columns_updated = self.analyze_schema_encoding(schema_encoding_tail_value)

            update_list = [None] * 5 # Initialize update_list with five None values

            for num in range(1, 5):
                if num in list_of_columns_updated:
                    physical_page_of_column = tail_page_to_work.get_page(num)
                    value_at_tail_record_column = physical_page_of_column.value_exists_at_bytes(indirection_base_value)
                    update_list[num] = value_at_tail_record_column
        
            for i in range(len(update_list) - 1):
                if columns_of_update[i] is not None:
                    update_list[i + 1] = columns_of_update[i]

            # inserts new tail record with previous tail record data (Cumulative)
            self.insert_tail_record(self.tid, new_schema_encoding, indirection_base_value, update_list)
        
        # update to the base page occurred 
        if base_page_to_work.update_indirection_base_column(self.tid, rid) and base_page_to_work.update_schema_encoding_base_column(new_schema_encoding, rid):
            return True
        return False
        
    # search base and tail pages list to find the page we are going to use  
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
        page_index = abs(rid) // (RECORDS_PER_PAGE + 1)
        return page_index + 1
        
    # No sure if this should go in table.py or in page_range.py    
    def get_schema_encoding(self,columns) -> int:
            schema_encoding = ''
            for item in columns:
                # if value in column is not 'None' add 1
                if item:
                    schema_encoding = schema_encoding + '1'
                # else add 0
                else:
                    schema_encoding = schema_encoding + '0'
            return int(schema_encoding, 2)
    
    # help determine what columns have been updated
    def analyze_schema_encoding(self,schema_encoding: int) -> list:
        """
        Analyzes a schema encoding represented as a 5-bit integer and returns a list
        containing the positions of bits with value 1, excluding the first bit.

        Args:
        - schema_encoding (int): The schema encoding represented as a 5-bit integer.

        Returns:
        - list: A list containing the positions of bits with value 1, excluding the first bit.
        """
        schema_encoding = abs(schema_encoding)
        if not isinstance(schema_encoding, int):
            raise TypeError("Schema encoding must be an integer.")
        
        if schema_encoding < 0 or schema_encoding >= 32:
            raise ValueError("Schema encoding must be a 5-bit integer (0-31).")

        # Initialize an empty list to store positions of bits with value 1
        positions = []
        
        # Iterate through each bit position (1 to 4) since key is never going to change
        for i in range(1, 5):
            # Check if the bit at position i is 1
            if schema_encoding & (1 << (4 - i)):
                positions.append(i)

        return positions
        
