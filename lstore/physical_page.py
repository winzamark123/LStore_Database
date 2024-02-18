from lstore.config import *
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
        self.updates = 0
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
        #print(f'Inserted value ({value}) in page ({self.column_number}) into Bytes ({start} - {end})')

        # Increment num_records only if update is False
        if not update:
            self.num_records += 1
        else:
            self.updates += 1

    # checks if a value is in physical page
    def check_value_in_page(self, value_to_find:int, rid:int)->bool:
        #print('\nFunction: check_value_in_page')
        offset = self.get_offset(rid)
        start = offset
        end = start + self.entry_size
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
        offset = self.get_offset(rid)
        start = offset
        end = start + self.entry_size
        entry_bytes = self.data[start:end]
        value_in_page = int.from_bytes(entry_bytes, byteorder='big', signed=True)
        # print(f"Value {value_in_page} was found at Bytes ({start} - {end}) in column {self.column_number}")
        return value_in_page

    # calculates our offset to know where the RID entry is at in the physical page
    def get_offset(self, rid:int)->int:
        if(rid < 0):
            rid = abs(rid)
        if self.column_number == 0:
            return (rid-1) * self.entry_size % INDIRECTION_PAGE_SIZE 
        return (rid-1) * self.entry_size % PHYSICAL_PAGE_SIZE        
    
