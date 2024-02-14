from lstore.page import *
from lstore.record import Record
from random import randint, randrange, choice, shuffle
from time import process_time
from lstore.page_range import * 

# To make it work, uncomment all the print() statements in page.py to see the physical pages get updated 

# Replace with your actual number of columns
num_columns = 5 

# key column 
key = 0 + META_DATA_NUM_COLUMNS

# These first 3 are for meta columns
entry_size_for_columns = [2,COLUMN_SIZE,COLUMN_SIZE]

# adds to list depending on how many columns are asked from user
for i in range(num_columns): 
    entry_size_for_columns.append(COLUMN_SIZE)

page_range = Page_Range(num_columns, entry_size_for_columns, key)
print('Base Page: ',page_range.base_pages[0].page_number)
print('Tail Page: ', page_range.tail_pages[0].page_number)

"""
INSERT RECORDS
"""

# Initialize a counter for records (RIDs)
record_counter = 1

# Initialize the index of the current base page
current_base_page_index = 0

base_page_amount = 3

insert_time_0 = process_time()
# Loop to keep adding records
while True:
    try:
        # Insert a new record into the current base page
        if page_range.base_pages[current_base_page_index].insert_new_record(Record(record_counter, 906659672 + record_counter, (randint(1,20), randint(1,20), randint(1,20), randint(1,20)))):
            print(f"Inserted record with RID {record_counter} into Base Page {current_base_page_index + 1}\n")
            record_counter += 1
        else:
            # If the current base page is full, add a new base page
            print(f"Base Page {current_base_page_index + 1} is full. Adding a new base page.")
            page_range.insert_base_page()

            # Move to the next base page
            current_base_page_index += 1
            
            # Stop creating base pages after base_page_amount
            if current_base_page_index == base_page_amount:
                break

    except IndexError:
        # If an index error occurs, it means we've reached the end of the base_pages list.
        # Add a new base page to accommodate the new record.
        if (len(page_range.base_pages) != base_page_amount):
            print("Index Error: Adding a new base page.")
            page_range.insert_base_page()


insert_time_1 = process_time()

print(f"Inserting records into {base_page_amount} base page(s) took:  \t\t\t", insert_time_1 - insert_time_0)



"""
UPDATE A CERTAIN RECORD

"""
print("\n\nUpdating Some Records!!!\n\n")

print(page_range.get_page_number(1025))


# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]


amount_of_records = 512 * base_page_amount
update_time_0 = process_time()

# Keep track of the number of records updated
records_updated = 0 

# updates record
while records_updated < amount_of_records:
    update_rid = randint(1,512 * base_page_amount)
    update_columns = choice(update_cols)
    page_range.update(rid=update_rid, columns_of_update=update_columns)
    records_updated += 1
    x = update_rid

update_time_1 = process_time()

print(f"Updating {records_updated} records took:  \t\t\t", update_time_1 - update_time_0)


print("\t\t\n\n\nReturn Record !!\n\n\n")

# return record wanted
return_record = page_range.return_record(x)

print(f'RID: {return_record.rid}')
print(f'KEY: {return_record.key}')
print(f'GRADES: {return_record.columns}')

for i in range(200):
    s = randint(0,4)
    print(f"Value at column {s} = {page_range.return_column_value(x, s)}")
