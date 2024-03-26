# record metadata columns
NUM_METADATA_COLUMNS = 4
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

# database configuration
RECORD_FIELD_SIZE = 8 # bytes
PHYSICAL_PAGE_SIZE = 4096 # bytes
NUM_RECORDS_PER_PAGE = 512 # records
NUM_BASE_PAGES_PER_PAGE_RANGE = 4 # base pages

# index configuration
INDEX_ORDER_NUMBER = 4

# bufferpool configuration
NUM_FRAMES_IN_BUFFERPOOL = 100

# merge configuration
MERGE_THRESHOLD = 1024
