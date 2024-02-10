COLUMN_SIZE = 8 # in bytes (8 bytes specified in paper pg. 546)
PHYSICAL_PAGE_SIZE = 4096 # in bytes
NUM_BASE_PAGES = 2# number of base pages
#PAGE_RANGE_SIZE = (PHYSICAL_PAGE_SIZE * ( TABLE_NUM_COLUMN * COLUMN_SIZE)) * NUM_BASE_PAGES  # how many bytes in our page range 
META_DATA_NUM_COLUMNS = 3 # columns that are meta data
RECORDS_PER_PAGE = PHYSICAL_PAGE_SIZE // 8 # number of records per page (512 Currently) 
INDIRECTION_PAGE_SIZE = 1024

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2