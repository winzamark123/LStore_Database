import lstore.config as Config
import os

class Physical_Page:
    """
    :param entry_size: int          # size of entry in bytes for this page (column)
    :param column_number: int       # number of this page (column) in the base page or tail page
    :param page_number: int         # page_number of this physical page corresponds to the base page it's on

    """
    def __init__(self, entry_size:int, column_index:int):
        # entry size of physical page entry differs between columns (RID column: 2 bytes, StudentID column: 8 bytes, etc..)
        self.num_records = 0
        self.entry_size = entry_size
        self.column_index = column_index
        self.updates = 0
        self.page_size = Config.PHYSICAL_PAGE_SIZE
        self.data = bytearray(Config.PHYSICAL_PAGE_SIZE)
    
    def __has_capacity(self)->bool:
        """
        # checks capacity of page
        """

        return ((self.num_records + 1) * self.entry_size) <= self.page_size and ((self.num_records + 1) <= Config.RECORDS_PER_PAGE)

    def __get_offset(self, rid:int)->int:
        """
        Calculates our offset to know where the RID entry is at in the physical page
        """

        if(rid < 0):
            rid = abs(rid)
        return (rid - 1) * self.entry_size % self.page_size

    def get_column_index(self):
        return self.column_index

    # write to physical page
    def write_to_physical_page(self, value:int, rid:int, update: bool=False)->None:
        # Perform capacity check only if update is False
        if not update and not self.__has_capacity():
            raise OverflowError("Not enough space in physical page or Reached record limit")

        # Gets offset with RID
        offset = self.__get_offset(rid)
        start = offset
        # Stop writing at (32 bytes + 8 bytes) = 40 bytes
        end = start + self.entry_size

        # Convert integer to (entry_size) bytes
        value_bytes = int.to_bytes(value, self.entry_size, byteorder='big', signed=True)
        self.data[start:end] = value_bytes
        print(f'Inserted value ({value}) in page ({self.column_index}) into Bytes ({start} - {end})')

        # Increment num_records only if update is False
        if not update:
            self.num_records += 1
        else:
            self.updates += 1

    # checks if a value is in physical page
    def check_value_in_page(self, value_to_find:int, rid:int)->bool:
        #print('\nFunction: check_value_in_page')
        offset = self.__get_offset(rid)
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
        offset = self.__get_offset(rid)
        start = offset
        end = start + self.entry_size
        entry_bytes = self.data[start:end]
        value_in_page = int.from_bytes(entry_bytes, byteorder='big', signed=True)
        return value_in_page

    #read physical page from disk
    def read_from_disk(self, path_to_physical_page: str, column_index: int):
        # Create directory if it doesn't exist
        directory = os.path.dirname(path_to_physical_page)
        if not os.path.exists(directory):
            os.makedirs(directory)

        physical_page_file = open(path_to_physical_page, "rb")
        physical_page_file.seek(column_index * Config.PHYSICAL_PAGE_SIZE)
        self.data = physical_page_file.read(Config.PHYSICAL_PAGE_SIZE)
        physical_page_file.close()

    #write physical page to disk
    def write_to_disk(self, path_to_physical_page: str, column_index: int):
        # Create directory if it doesn't exist
        directory = os.path.dirname(path_to_physical_page)
        if not os.path.exists(directory):
            os.makedirs(directory)

        physical_page_file = open(path_to_physical_page, "wb")
        physical_page_file.seek(column_index * Config.PHYSICAL_PAGE_SIZE)
        physical_page_file.write(self.data)
        physical_page_file.close()
