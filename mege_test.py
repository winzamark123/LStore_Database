from lstore.page import *
from lstore.record import Record
from time import process_time
from random import randint, randrange, choice, shuffle
from lstore.page_range import * 
import copy

# To make it work, uncomment all the print() statements in page.py to see the physical pages get updated 

# Replace with your actual number of columns
num_columns = 5 

# key column 
key = 0 + META_DATA_NUM_COLUMNS

# These first 3 are for meta columns
entry_size_for_columns = [2,COLUMN_SIZE,COLUMN_SIZE, COLUMN_SIZE]

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

updated_rids = []

# updates record
while records_updated < 5:
    update_rid = randint(1,10)
    update_columns = choice(update_cols)
    if (page_range.update(rid=update_rid, columns_of_update=update_columns)):
        records_updated += 1
        updated_rids.append(update_rid)
    else: 
        print('update came back false\n')


update_time_1 = process_time()

print(f"Updating {records_updated} records took:  \t\t\t", update_time_1 - update_time_0)


print("\t\t\n\n\nReturn Record !!\n\n\n")

print(updated_rids)

# return_record = 0

# for rids in updated_rids:
#     return_record = page_range.return_record(rids)
#     print(f'\n\nRID: {return_record.rid}')
#     print(f'KEY: {return_record.key}')
#     print(f'GRADES: {return_record.columns}')


@staticmethod
def __merge(page_ranges:Page_Range):
    
    # records that need updating in base page
    updated_records = {}

    # iterate backwards through tail pages list
    for tail_page in reversed(page_ranges.tail_pages):

        # base_rid and tid page in tail page
        base_rid_page = tail_page.get_base_rid_page()
        tid_page = tail_page.physical_pages[1]

        amount_records = tid_page.num_records
        
        print(tail_page.num_records * COLUMN_SIZE)
        for i in range(PHYSICAL_PAGE_SIZE - COLUMN_SIZE, -1, -COLUMN_SIZE):
            # Extract 8 bytes at a time using slicing
            base_rid_for_tail_record_bytes = base_rid_page.data[i:i+COLUMN_SIZE]

            base_rid_for_tail_record_value = int.from_bytes(base_rid_for_tail_record_bytes, byteorder='big', signed=True)

            # breaks out of loop if 0 meaning it's reached end of tail records in a tail page
            if base_rid_for_tail_record_value == 0:
                continue

            tid_for_tail_record_bytes = tid_page.data[i:i+COLUMN_SIZE]

            print(tid_for_tail_record_bytes)

            tid_for_tail_record_value = -(int.from_bytes(tid_for_tail_record_bytes, byteorder='big', signed=True))

            print(tid_for_tail_record_value)
            
            print(base_rid_for_tail_record_value)
            # adds rid if rid is not in update_records dictionary
            if base_rid_for_tail_record_value not in updated_records.values():
                updated_records[-(tid_for_tail_record_value)] = base_rid_for_tail_record_value 

    
    for key, value in updated_records.items():
        print(f'\nDict Pair: {key} = {value}')

s = copy.deepcopy(page_range)
__merge(s)

for base_page in page_range.tail_pages:
    print(base_page.num_records)
