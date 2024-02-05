import time
from lstore.config import *
from lstore.record import Record

class Physical_Page:
    """
    :param entry_size: int          # size of entry in bytes for this page (column)
    :param column_number: int       # number of this page (column) in the base page or tail page
    :param page_number: int         # page_number of this physical page corresponds to the base page it's on

    """
    def __init__(self, entry_size:int, column_number:int):
        self.num_records = 0
        self.data = bytearray(PHYSICAL_PAGE_SIZE)
        self.entry_size = entry_size # entry size of physical page entry differs between columns (RID colum: 2 bytes, StudentID column: 8 bytes, etc..)
        self.column_number = column_number 

    def has_capacity(self)->bool:
        return ((self.num_records + 1) * self.entry_size <= PHYSICAL_PAGE_SIZE) and ((self.num_records + 1) <= RECORDS_PER_PAGE)

    # write to physical page
    def write_to_physical_page(self, value:int, rid:int)->None:
        print('\nFunction: write_to_physical_page')
        if self.has_capacity():
            offset = self.get_offset(rid) # gets offset with RID
            start = offset
            end = start + self.entry_size # ex : stop writing at (32 bytes + 8 bytes) = 40 bytes
            value_bytes = int.to_bytes(value, self.entry_size, byteorder='big') # converts integer to (entry_size) bytes
            self.data[start:end] = value_bytes
            print(f'Inserted value ({value}) in page ({self.column_number}) into Bytes ({start} - {end})')
            self.num_records += 1
        else:
            raise OverflowError("Not enough space in physical page or Reached record limit")

    # checks if a value is in physical page
    def check_value_in_page(self, value_to_find:int, rid:int)->bool:
        print('\nFunction: check_value_in_page')
        offset = self.get_offset(rid)
        start = offset
        end = start + self.entry_size
        entry_bytes = self.data[start:end]
        entry_value = int.from_bytes(entry_bytes, byteorder='big') # converts bytes to integer
        if(entry_value == value_to_find): # if value in entry_bytes match value we're trying to find, then we print that we found it (we can change it to return True or false)
            print(f"Value {entry_value} was found at Bytes ({start} - {end})")
            return True
        else:
            print(f"value {entry_value} was not found at Bytes ({start} - {end})")
            return False

    # using for testing 
    def value_exists_at_bytes(self, rid:int):
        print('\nFunction: value_exists_at_bytes()')
        offset = self.get_offset(rid)
        start = offset
        end = start + self.entry_size
        entry_bytes = self.data[start:end]
        value_in_page = int.from_bytes(entry_bytes, byteorder='big')
        print(f"Value {value_in_page} was found at Bytes ({start} - {end})")

    # calculates our offset to know where the RID entry is at in the physical page
    def get_offset(self, rid:int)->int:
        if(rid < 0):
            rid = abs(rid)
        return (rid-1) * self.entry_size % PHYSICAL_PAGE_SIZE        
    

class Base_Page:
    """
    :param num_columns: int         # amount of columns in table (5 columns for our example)
    :param entry_sizes: list        # list of the entry sizes ([2 bytes, 8 bytes, ...]) for each physical page
    :param key_column: int          # column (physical page) the key value is going to be at 

    """
    
    def __init__(self, num_columns:int, entry_sizes:list, key_column:int)->None:

        # list of physical_pages in base page
        self.physical_pages = [] 

        # column (physical page) the key value is going to be at 
        self.primary_key_column = key_column

        # adds to list of physical_pages depending the amount of columns
        for column_number ,entry_size in enumerate(entry_sizes, start=0): 
            column_page = Physical_Page(entry_size=entry_size, column_number=column_number)
            self.physical_pages.append(column_page)

    def get_primary_key_page(self)->Physical_Page: # returns physical_page(column) that has the primary keys 
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == self.primary_key_column ):
                #print(f'column for primary key page : ({physical_Page.column_number})')
                return physical_Page

    def get_rid_page(self)->Physical_Page: # returns physical_page(column) that has the RIDs
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == RID_COLUMN):
                return physical_Page

    def get_indirection_page(self)->Physical_Page: # returns physical_page(column) that has the Indirections
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == INDIRECTION_COLUMN):
                return physical_Page

    def get_page(self, column_number:int)->Physical_Page: # returns page needed
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == (META_DATA_NUM_COLUMNS + column_number)):
                return physical_Page

    def value_getting_updated(self, key_value:int, column_to_update:int, new_value:int, rid:int)->bool: # updates value in physical pages 
        print('\nFunction: value_getting_updated()')
        key_page = self.get_primary_key_page() # grabs key page (Student IDs)

        # checks if key(Student ID) exists
        if(key_page.check_value_in_page(key_value,rid)): 

            # physical page of column we want update
            column_to_update_page = self.get_page(column_to_update) 

            # update value with new value
            column_to_update_page.write_to_physical_page(new_value, rid)
            print('\nvalue was updated')
            return True
        else:
            # key value(Student ID) does not exist
            print(f'Key {key_value} does not exist')
            return False

    # gets RID that's associated with a key (Student ID)
    def get_rid_for_key(self, key_value:int)->int:
        print('\nFunction: get_rid_for_key()')
        key_page = self.get_primary_key_page()

        # Check if key exists in the primary key page
        for rid in range(1, RECORDS_PER_PAGE + 1):
            if key_page.check_value_in_page(key_value, rid):
                return rid

        # If key is not found, raise a KeyError
        raise KeyError(f"Key {key_value} not found in the primary key column.")

    # In the works
    def insert_new_base_record(self, new_record: Record):

        # rid of record 
        rid = new_record.rid
        
        # grabs rid page
        rid_page = self.get_rid_page()

        # checks if RID exists
        if(rid_page.check_value_in_page(rid,rid) == False): 

            # key of record (Student ID)
            key = new_record.key

            # grabs key page
            key_page = self.get_primary_key_page()

            # write to key page 
            key_page.write_to_physical_page(key,rid)

            # write to RID page
            rid_page.write_to_physical_page(rid,rid)

            # columns of record
            columns = new_record.columns
        
            for column_num, column_data in enumerate(columns, start = 1):
                page_to_write = self.get_page(column_number=column_num) # gets page which corresponds to each column
                page_to_write.write_to_physical_page(value=column_data, rid=rid)
            print("\nRecord Inserted!")
        else:
            raise KeyError("RID already exists.")

        
        """
        for i, physical_pages in enumerate(self.columns):
            # create a new empty physical page if last one is full
            if not physical_pages[-1].has_capacity(): physical_pages.append(Physical_Page())
            
            # write an entry's column value
            physical_pages[-1].write_to_physical_page(entry_columns[i])
        
        # add entry's metadata
        self.metadata[len(self.metadata)+1] = [None, bytearray(len(entry_columns)), time.time()] # maybe add last updated time?
        """

class Tail_Page:
    def __init__(self, num_columns:int)->None:
        self.columns = [Physical_Page()] * num_columns