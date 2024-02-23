COLUMN_SIZE = 8 # in bytes (8 bytes specified in paper pg. 546)
PHYSICAL_PAGE_SIZE = 4096 # in bytes
NUM_BASE_PAGES = 7# number of base pages
ORDER_CHOICE = 3 # order of the B+ tree
#PAGE_RANGE_SIZE = (PHYSICAL_PAGE_SIZE * ( TABLE_NUM_COLUMN * COLUMN_SIZE)) * NUM_BASE_PAGES  # how many bytes in our page range 
META_DATA_NUM_COLUMNS = 3 # columns that are meta data

RECORDS_PER_PAGE = PHYSICAL_PAGE_SIZE // COLUMN_SIZE 
INDIRECTION_PAGE_SIZE = 1024

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
SCHEMA_ENCODING = 2

BUFFER_SIZE = 100