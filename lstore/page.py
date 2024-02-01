import time

COLUMN_SIZE = 8 # in bytes (8 bytes specified in paper pg. 546)
PHYSICAL_PAGE_SIZE = 4096 # in bytes
NUM_BASE_PAGES = 16
PAGE_RANGE_SIZE = PHYSICAL_PAGE_SIZE * NUM_BASE_PAGES

class Physical_Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PHYSICAL_PAGE_SIZE)

    def has_capacity(self)->bool:
        return (self.num_records + 1) * COLUMN_SIZE < PHYSICAL_PAGE_SIZE

    def write_to_physical_page(self, value:str): # value has to be a string in order to properly encode the value into the physical page
        if len(value.encode()) < COLUMN_SIZE: raise OverflowError
        self.data[self.num_records:self.num_records+COLUMN_SIZE] = bytearray(COLUMN_SIZE - len(bytearray(value.encode()))) + value.encode()
        self.num_records += 1


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

        self.metadata = []
        self.columns = [[Physical_Page()]] * num_columns
    
    def write_to_base_page(self, *entry_columns)->None:
        for i, physical_pages in enumerate(self.columns):
            # create a new empty physical page if last one is full
            if not physical_pages[-1].has_capacity(): physical_pages.append(Physical_Page())
            
            # write an entry's column value
            physical_pages[-1].write_to_physical_page(entry_columns[i])
        
        # add entry's metadata
        self.metadata[len(self.metadata)+1] = [None, bytearray(len(entry_columns)), time.time()] # maybe add last updated time?

class Tail_Page:
    
    def __init__(self, num_columns:int)->None:
        self.columns = [Physical_Page()] * num_columns

class Page_Range:

    def __init__(self):
        pass