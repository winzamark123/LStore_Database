""" physical_page (PP) the unit of storage """

from lstore.record import RID
import lstore.config as Config


class PhysicalPage:
    """physical page class for lstore"""

    def __init__(self):
        # self.num_records = 0
        # entry size of physical page entry differs between columns (RID colum: 2 bytes, StudentID column: 8 bytes, etc..)
        # self.entry_size = DATA_ENTRY_SIZE
        self.data = bytearray(Config.PHYSICAL_PAGE_SIZE)

    @classmethod
    def from_bytes(cls, bytes_data: bytes):
        # Create an instance with default or specified values
        instance = cls()
        # Now fill in the data with the bytes provided
        instance.data = bytearray(bytes_data)
        # Calculate the number of records based on the actual bytes length and entry_size (assuming full entries)
        return instance

    # write to physical page
    def edit_byte_array(self, value: int, rid: int) -> None:
        """edit the byte array of the physical page"""
        offset = self.__get_offset(rid)
        bytes_to_insert = value.to_bytes(
            length=Config.DATA_ENTRY_SIZE, byteorder="big", signed=True
        )
        self.data = (
            self.data[:offset]
            + bytes_to_insert
            + self.data[offset + len(bytes_to_insert) :]
        )

    def __get_byte_array(self, rid: int) -> bytearray:
        offset = self.__get_offset(rid)
        return self.data[offset : offset + Config.DATA_ENTRY_SIZE]

    def get_data(self, rid: RID) -> int:
        """get data from self.data in pp"""
        return int.from_bytes(self.__get_byte_array(rid), byteorder="big", signed=True)

    # checks if a value is in physical page
    def check_value_in_page(self, value_to_find: int, rid: int) -> bool:
        """check if value is in the physical page"""
        offset = self.__get_offset(rid)
        start = offset
        end = start + Config.DATA_ENTRY_SIZE
        entry_bytes = self.data[start:end]
        # converts bytes to integer
        entry_value = int.from_bytes(entry_bytes, byteorder="big")

        # if value in entry_bytes match value we're trying to find, then we print that we found it (we can change it to return True or false)
        if entry_value == value_to_find:
            # print(f"Value {entry_value} was found at Bytes ({start} - {end})")
            return True
        else:
            # print(f"value {entry_value} was not found at Bytes ({start} - {end})")
            return False

    def value_exists_at_bytes(self, rid: int) -> int:
        # print('\nFunction: value_exists_at_bytes()')
        offset = self.__get_offset(rid)
        start = offset
        end = start + Config.DATA_ENTRY_SIZE
        entry_bytes = self.data[start:end]
        value_in_page = int.from_bytes(entry_bytes, byteorder="big", signed=True)
        return value_in_page

    # calculates our offset to know where the RID entry is at in the physical page
    def __get_offset(self, rid: int) -> int:

        if isinstance(rid, RID):
            rid = rid.to_int()

        if rid < 0:
            rid = abs(rid)

        return (rid - 1) * Config.DATA_ENTRY_SIZE % Config.PHYSICAL_PAGE_SIZE
