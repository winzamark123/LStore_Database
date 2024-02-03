import time
from lstore.config import *

class Physical_Page:
    """
    :param entry_size: int          # size of entry in bytes for this page (column)
    :param column_number: int       # number of this page (column) in the base page or tail page
    :param page_number: int         # page_number of this physical page corresponds to the base page it's on

    """
    def __init__(self, entry_size:int, column_number:int, page_number:int):
        self.num_records = 0
        self.data = bytearray(PHYSICAL_PAGE_SIZE)
        self.entry_size = entry_size # entry size of physical page entry differs between columns (Indirection colum: 2 bytes, StudentID column: 8 bytes, etc..)
        self.column_number = column_number 
        self.page_number = page_number # know what base page this physical page belongs to (If base page and physical page_number match then physical page is in base page )

    def has_capacity(self)->bool:
        return ((self.num_records + 1) * self.entry_size <= PHYSICAL_PAGE_SIZE) and ((self.num_records + 1) <= RECORDS_PER_PAGE)

    def write_to_physical_page(self, value:int): # value has to be a string in order to properly encode the value into the physical page
        if self.has_capacity():
            start = self.num_records * self.entry_size # ex : 4 record entries for StudentID column (physical page), then we start writing at (4 record entries * 8 bytes = 32 bytes)
            end = start + self.entry_size # ex : stop writing at 32 bytes + 8 bytes = 40 bytes
            value_bytes = int.to_bytes(value, self.entry_size, byteorder='big') # converts integer to (entry_size) bytes
            self.data[start:end] = value_bytes
            self.num_records += 1
        else:
            raise OverflowError("Not enough space in physical page or Reached record limit")

    def find_value_in_page(self, value_to_find:int):
        for i in range(0, len(self.data), self.entry_size):
            entry_bytes = self.data[i:i + self.entry_size]
            entry_value = int.from_bytes(entry_bytes, byteorder='big')
            if(value_to_find == entry_value):
                return entry_value
            #print(f"Entry {i // self.entry_size + 1}: {entry_value}")
        
        raise Exception("Value was not found")
    

class Base_Page:
    
    def __init__(self, num_columns:int)->None:
        """
        Metadata:
            - Indirection Value: 8 bytes
                (from paper: "The Indirection column is at most 8-byte long" (pg. 547))
            - RID: 2 bytes
                (from paper: "Base RID is a highly compressible column that would require at most two bytes")
            - Schema Encoding: (# columns) bits
            - Timestamp: however many bytes the Python time object takes up
                - Atomic Time? https://stackoverflow.com/questions/17979375/how-do-i-get-the-atomic-clock-time-in-python
        
        Entry Columns (key, etc.):

        |------------------- metadata -------------------|----- entry ------|       
        | RID | Indir. | Schema Encoding | Creation Time | key | col1 | ... |
        |-----|--------|-----------------|---------------|-----|------|-...-|
        | 2   | 8      | # columns       | ?             | 8   | 8    | ... | number of bytes each column will take (metadata reqs only depend on if we want to follow the paper's info)
        ...

        Convert this into a list of columns w/ each column represented by physical pages
        [col1 entry list, ..., list(Physical Pages)]
        
        Metadata will contain: (TODO: figure out if this is correct)
        | RID (starts @ 1) | Indirection | SE | CT | (last updated time)? |
        RID can can be associated to a Physical Page's index by performing (RID - 1) * COLUMN_SIZE // PHYSICAL_PAGE_SIZE
        Offset is found w/ (RID-1) * COLUMN_SIZE % PHYSICAL_PAGE_SIZE
        """

        self.entry_and_metadata_columns = [[Physical_Page()] * (META_DATA_NUM_COLUMNS + num_columns)] # creates physical pages(columns of entry plus meta columns) inside base page    


    def write_to_base_page(self, *entry_columns)->None:
        for i, physical_pages in enumerate(self.columns):
            # create a new empty physical page if last one is full
            if not physical_pages[-1].has_capacity(): physical_pages.append(Physical_Page())
            
            # write an entry's column value
            physical_pages[-1].write_to_physical_page(entry_columns[i])
        
        # add entry's metadata
        self.metadata[len(self.metadata)+1] = [None, bytearray(len(entry_columns)), time.time()] # maybe add last updated time?

    def access_physical_page(self, entry_for_colum:int)->None: # be able to access physical pages inside the base pages and tail pages 
        for physical_page in self.entry_and_metadata_columns: # looks at every page in base page 
             print(physical_page)
        
class Tail_Page:
    def __init__(self, num_columns:int)->None:
        self.columns = [Physical_Page()] * num_columns



