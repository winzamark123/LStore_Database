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
        # entry size of physical page entry differs between columns (RID colum: 2 bytes, StudentID column: 8 bytes, etc..)
        self.entry_size = entry_size 
        self.column_number = column_number 
        if(column_number == 0):
            self.data = bytearray(INDIRECTION_PAGE_SIZE) # Indirection column is smaller
        else:
            self.data = bytearray(PHYSICAL_PAGE_SIZE)

    # checks capacity of page
    def has_capacity(self)->bool:
        if(self.column_number == 0):
            return ((self.num_records + 1) * self.entry_size <= INDIRECTION_PAGE_SIZE) and ((self.num_records + 1) <= RECORDS_PER_PAGE)
        else:
            return ((self.num_records + 1) * self.entry_size <= PHYSICAL_PAGE_SIZE) and ((self.num_records + 1) <= RECORDS_PER_PAGE)


    # write to physical page
    def write_to_physical_page(self, value:int, rid:int, update: bool=False)->None:
        # Perform capacity check only if update is False
        if not update and not self.has_capacity():
            raise OverflowError("Not enough space in physical page or Reached record limit")

        # Gets offset with RID
        offset = self.get_offset(rid) 
        start = offset
        # Stop writing at (32 bytes + 8 bytes) = 40 bytes
        end = start + self.entry_size

        # Convert integer to (entry_size) bytes
        value_bytes = int.to_bytes(value, self.entry_size, byteorder='big', signed=True)
        self.data[start:end] = value_bytes
        print(f'Inserted value ({value}) in column ({self.column_number}) into Bytes ({start} - {end})')

        # Increment num_records only if update is False
        if not update:
            self.num_records += 1

    # checks if a value is in physical page
    def check_value_in_page(self, value_to_find:int, rid:int)->bool:
        print('\nFunction: check_value_in_page')
        offset = self.get_offset(rid)
        start = offset
        end = start + self.entry_size
        entry_bytes = self.data[start:end]
        # converts bytes to integer
        entry_value = int.from_bytes(entry_bytes, byteorder='big')

        # if value in entry_bytes match value we're trying to find, then we print that we found it (we can change it to return True or false)
        if(entry_value == value_to_find):
            print(f"Value {entry_value} was found at Bytes ({start} - {end})")
            return True
        else:
            print(f"value {entry_value} was not found at Bytes ({start} - {end})")
            print("============================")
            return False

    # using for testing 
    def value_exists_at_bytes(self, rid:int)->int:
        print('\nFunction: value_exists_at_bytes()')
        offset = self.get_offset(rid)
        start = offset
        end = start + self.entry_size
        entry_bytes = self.data[start:end]
        value_in_page = int.from_bytes(entry_bytes, byteorder='big')
        print(f"Value {value_in_page} was found at Bytes ({start} - {end})")
        return value_in_page

    # calculates our offset to know where the RID entry is at in the physical page
    def get_offset(self, rid:int)->int:
        if(rid < 0):
            rid = abs(rid)
        if self.column_number == 0:
            return (rid-1) * self.entry_size % INDIRECTION_PAGE_SIZE 
        return (rid-1) * self.entry_size % PHYSICAL_PAGE_SIZE        
    

class Page:
    """
    :param num_columns: int         # amount of columns in table (5 columns for our example)
    :param entry_sizes: list        # list of the entry sizes ([2 bytes, 8 bytes, ...]) for each physical page
    :param key_column: int          # column (physical page) the key value is going to be at 

    """
    # Class variables to keep track of the number of Base_Page and Tail_Page objects created
    base_page_counter = 0
    tail_page_counter = 0

    def __init__(self, num_columns:int, entry_sizes:list, key_column:int, is_tail_page: bool = False)->None:
        if is_tail_page:
            # Increment the tail_page_counter each time a Tail_Page object is created
            Page.tail_page_counter += 1
            self.page_number = Page.tail_page_counter
        else:
            # Increment the base_page_counter each time a Base_Page object is created
            Page.base_page_counter += 1
            self.page_number = Page.base_page_counter


        # list of physical_pages in base page
        self.physical_pages = [] 

        # column (physical page) the key value is going to be at 
        self.primary_key_column = key_column 

        # adds to list of physical_pages depending the amount of columns
        for column_number ,entry_size in enumerate(entry_sizes, start=0): 
            column_page = Physical_Page(entry_size=entry_size, column_number=column_number)
            self.physical_pages.append(column_page)

        self.num_records = 0
        print("Physical Pages:", self.physical_pages)

    # returns physical_page(column) that has the primary keys 
    def get_primary_key_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            # print("PHYSICAL PAGE COL", physical_Page.column_number)
            # print("Primary Key Col", self.primary_key_column)

            if(physical_Page.column_number == self.primary_key_column ):
                # print("Returning primary key page")
                return physical_Page

    # returns physical_page(column) that has the RIDs
    def get_rid_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == RID_COLUMN):
                return physical_Page

    # returns physical_page(column) that has the Indirections
    def get_indirection_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == INDIRECTION_COLUMN):
                return physical_Page

    # returns page needed
    def get_page(self, column_number:int)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == (META_DATA_NUM_COLUMNS + column_number)):
                return physical_Page

    # updates value in physical pages 
    def value_getting_updated_base_page(self, key_value:int, column_to_update:int, new_value:int, rid:int, tail_record_lid:int)->bool:
        key_page = self.get_primary_key_page() # grabs key page (Student IDs)

        # checks if key(Student ID) exists
        if key_page.check_value_in_page(key_value,rid): 

            # physical page of column we want update
            column_to_update_page = self.get_page(column_to_update) 

            # update value with new value
            column_to_update_page.write_to_physical_page(new_value, rid, update=True)

            # indirection column updated to have most recent version of record
            indirection_page = self.get_indirection_page()

            # updates indirection column for record
            indirection_page.write_to_physical_page(tail_record_lid, rid, update=True)

            return True
        
        # key value(Student ID) does not exist
        print('key value does not match with rid')
        return False

    def value_getting_updated_tail_page(self, column_to_update:int, new_value:int, rid:int)->bool:

            # physical page of column we want update in tail page
            column_to_update_page = self.get_page(column_to_update) 

            # update with new value
            column_to_update_page.write_to_physical_page(new_value, rid, update=True)
            return True

            print("Did not update in tail page")

    # gets RID that's associated with a key (Student ID)
    def get_rid_for_key(self, key_value:int)->int:
        key_page = self.get_primary_key_page()
        entry_size = key_page.entry_size

        rid_page = self.get_rid_page()

        # Iterate over the entire byte array
        for offset in range(0, len(key_page.data), entry_size):
            # Read the value at the current offset
            entry_bytes = key_page.data[offset:offset+entry_size]
            entry_value = int.from_bytes(entry_bytes, byteorder='big')

            # Check if the entry value matches the key value
            if entry_value == key_value:
                # Calculate the RID based on the offset
                rid_to_look = (offset // entry_size) + 1
                rid_to_return = rid_page.value_exists_at_bytes(rid_to_look)
                return rid_to_return

        # If key is not found, raise a KeyError
        raise KeyError(f"Key {key_value} not found in the primary key column.")

    # inserts new base record into base page
    def insert_new_record(self, new_record: Record)->bool:

        # checks if page is full of records
        if not self.has_capacity():
            return False
        
        # rid of record 
        rid = new_record.rid
        
        # grabs rid page
        rid_page = self.get_rid_page()

        print(f'RID ({rid} getting inserted)')
        # checks if RID exists)
        if(rid_page.check_value_in_page(rid,rid) == False): 

            # key of record (Student ID)
            key = new_record.key

            # grabs key page
            key_page = self.get_primary_key_page()
            print(f'Key ({key}) getting inserted')
            print(f'Key Page ({key_page})')

            # write to key page 
            key_page.write_to_physical_page(key,rid)

            # grabs Indirection page
            indirection_page = self.get_indirection_page()

            # write to Indirection page - indirection set to equal to RID because it's the most previous version of that record
            indirection_page.write_to_physical_page(rid, rid)

            # write to RID page
            rid_page.write_to_physical_page(rid,rid)

            # columns of record
            columns = new_record.columns
        
            for column_num, column_data in enumerate(columns, start = 1):
                # gets page which corresponds to each column
                page_to_write = self.get_page(column_number=column_num) 
                page_to_write.write_to_physical_page(value=column_data, rid=rid)
            print("\nRecord Inserted!")
            self.num_records += 1
            return True
        else:
            raise KeyError(f"RID ({rid}) already exists.")


    # returns record with just the RID
    def get_record_with_rid(self,rid:int)->Record:
        print('\nFunction: get_record_with_rid()')

        # Create variables to store the attributes
        key = None

        # creates data tuple
        data = ()

        # RID page
        rid_page = self.get_rid_page()

        # checks if RID exists in this page
        if (rid_page.check_value_in_page(rid,rid)):
            # Iterate through the physical pages
            for column_page in self.physical_pages:
                # Read the value corresponding to the RID in each column page
                offset = column_page.get_offset(rid)
                start = offset
                end = start + column_page.entry_size
                entry_bytes = column_page.data[start:end]
                entry_value = int.from_bytes(entry_bytes, byteorder='big')

                # Assign the retrieved attribute to the appropriate variable based on the column number
                if column_page.column_number == self.primary_key_column:
                    key = entry_value
                elif column_page.column_number > META_DATA_NUM_COLUMNS and column_page.column_number != self.primary_key_column:
                    data = data + (entry_value,)
                # Add more conditions if there are additional columns

            # Create and return the Record object with the retrieved attributes
            return Record(rid, key, data)
        
        raise KeyError(f'RID ({rid}) does not exists in this base page')

    # checks if RID is in Base_page or Tail_Page
    def check_for_rid(self, rid:int)->bool:
        rid_page = self.get_rid_page()

        # returns if RID is in page
        return rid_page.check_value_in_page(rid,rid) 

    # checks if page is full
    def has_capacity(self)->bool:
        if self.num_records == RECORDS_PER_PAGE:
            print(f'Page is full of records on base_page ({self.page_number})')
            return False 
        
        return True 

        

class Tail_Page(Page):
    def __init__(self, num_columns:int, entry_sizes:list, key_column:int)->None:
        # Call the constructor of the parent class (Page)
        super().__init__(num_columns, entry_sizes, key_column, is_tail_page=True)

class Base_Page(Page):
    def __init__(self, num_columns:int, entry_sizes:list, key_column:int, tail_page: Tail_Page)->None:
        # Call the constructor of the parent class (Page)
        super().__init__(num_columns, entry_sizes, key_column, is_tail_page=False)

        # tail page associated with this base page
        self.tail_page = tail_page
