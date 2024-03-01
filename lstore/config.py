COLUMN_SIZE = 8 # in bytes (8 bytes specified in paper pg. 546)
PHYSICAL_PAGE_SIZE = 4096 # in bytes
NUM_BASE_PAGES = 4# number of base pages
ORDER_CHOICE = 4 # order of the B+ tree
#PAGE_RANGE_SIZE = (PHYSICAL_PAGE_SIZE * ( TABLE_NUM_COLUMN * COLUMN_SIZE)) * NUM_BASE_PAGES  # how many bytes in our page range 
META_DATA_NUM_COLUMNS = 4 # columns that are meta data

RECORDS_PER_PAGE = PHYSICAL_PAGE_SIZE // COLUMN_SIZE 

MERGE_THRESHOLD = 1024 # amounts of updates in page range to trigger merge
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
BASE_RID_COLUMN = 3
# 1 frame = 1 physical page  
BUFFERPOOL_FRAME_SIZE = 100 # number of frames in bufferpool
DATA_ENTRY_SIZE = 8 # size of data entry in bytes
