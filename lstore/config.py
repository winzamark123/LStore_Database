COLUMN_SIZE = 8 # in bytes (8 bytes specified in paper pg. 546)
PHYSICAL_PAGE_SIZE = 4096 # in bytes
NUM_BASE_PAGES = 16 # 16 base pages in our page range 
#PAGE_RANGE_SIZE = (PHYSICAL_PAGE_SIZE * ( TABLE_NUM_COLUMN * COLUMN_SIZE)) * NUM_BASE_PAGES  # how many bytes in our page range 
META_DATA_NUM_COLUMNS = 4 # columns that are meta data
RECORDS_PER_PAGE = PHYSICAL_PAGE_SIZE // 8 

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
