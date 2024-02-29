import os
from lstore.record import RID
from lstore.config import *

class Physical_Page:
    """
    :param entry_size: int          # size of entry in bytes for this page (column)
    :param column_number: int       # number of this page (column) in the base page or tail page
    :param page_number: int         # page_number of this physical page corresponds to the base page it's on

    """

    def __init__(self):
        # self.num_records = 0
        # entry size of physical page entry differs between columns (RID colum: 2 bytes, StudentID column: 8 bytes, etc..)
        # self.entry_size = DATA_ENTRY_SIZE 
        self.data = bytearray(PHYSICAL_PAGE_SIZE)
    
    @classmethod
    def from_bytes(cls, bytes_data: bytes):
         # Create an instance with default or specified values
        instance = cls()
        # Now fill in the data with the bytes provided
        instance.data = bytearray(bytes_data)
        # Calculate the number of records based on the actual bytes length and entry_size (assuming full entries)
        return instance

    # checks capacity of page
    def has_capacity(self)->bool:
        return ((self.num_records + 1) * DATA_ENTRY_SIZE <= PHYSICAL_PAGE_SIZE) and ((self.num_records + 1) <= RECORDS_PER_PAGE)

    # write to physical page
    def edit_byte_array(self, value:int, rid:int)->None:
        # Gets offset with RID
        offset = self.__get_offset(rid)
        start = offset

        print("OFFSET", offset)
        print("RID", rid)
        # Stop writing at (32 bytes + 8 bytes) = 40 bytes
        end = start + DATA_ENTRY_SIZE 

        # Convert integer to (entry_size) bytes
        value_bytes = int.to_bytes(value, DATA_ENTRY_SIZE, byteorder='big', signed=True)
        self.data[start:end] = value_bytes
        # print(f'Inserted value ({value}) in page ({self.column_index}) into Bytes ({start} - {end})')


    # checks if a value is in physical page
    def check_value_in_page(self, value_to_find:int, rid:int)->bool:

        offset = self.__get_offset(rid)
        start = offset
        end = start + DATA_ENTRY_SIZE 
        entry_bytes = self.data[start:end]
        # converts bytes to integer
        entry_value = int.from_bytes(entry_bytes, byteorder='big')

        # if value in entry_bytes match value we're trying to find, then we print that we found it (we can change it to return True or false)
        if(entry_value == value_to_find):
            #print(f"Value {entry_value} was found at Bytes ({start} - {end})")
            return True
        else:
            #print(f"value {entry_value} was not found at Bytes ({start} - {end})")
            return False

    def value_exists_at_bytes(self, rid:int)->int:
        # print('\nFunction: value_exists_at_bytes()')
        offset = self.__get_offset(rid)
        start = offset
        end = start + DATA_ENTRY_SIZE
        entry_bytes = self.data[start:end]
        value_in_page = int.from_bytes(entry_bytes, byteorder='big', signed=True)
        return value_in_page

    # calculates our offset to know where the RID entry is at in the physical page
    def __get_offset(self, rid:int)->int:
        if(rid < 0):
            rid = abs(rid)
        return (rid-1) * DATA_ENTRY_SIZE % PHYSICAL_PAGE_SIZE        
    
        
    
