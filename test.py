from lstore.page import *
from lstore.record import Record

# To make it work, uncomment all the print() statements in page.py to see the physical pages get updated 

# Replace with your actual number of columns
num_columns = 5 

# key column
key = 0

# These first 4 are for meta columns
entry_size_for_columns = [2,8,8,8]

# adds to list depending on how many columns are asked from user
for i in range(num_columns): 
    entry_size_for_columns.append(8) 


# creates base page
base_page = Base_Page(num_columns, entry_size_for_columns, META_DATA_NUM_COLUMNS + key)
print("\nInserting Record(RID=22,KEY=906659672,Grades(10,15,8,7))\n")
base_page.insert_new_base_record(Record(22,906659672, (10,15,8,7)))