from lstore.page import *

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

# key_column page
key_column = base_page.get_primary_key_page()

# write to key column a random stID with RID
key_column.write_to_physical_page(906659671, 20)

# writing to physical page which has a column number (4 + 3) = 7, which is the 3rd grade for our example
for physical_page in base_page.physical_pages:
    if (physical_page.column_number == META_DATA_NUM_COLUMNS + 3):
        physical_page.write_to_physical_page(13,20)

# update grade 3 for the (Student ID: 906659671) to 15 
base_page.value_getting_updated(906659671, 3, 15, 20)