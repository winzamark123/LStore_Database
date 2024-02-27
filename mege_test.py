from lstore.page import *
from lstore.record import Record
from time import process_time
from random import randint, randrange, choice, shuffle
from lstore.page_range import * 
import copy
import threading

from lstore.bufferpool import Bufferpool

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
while records_updated < 2048:
    update_rid = randint(1, 512 * base_page_amount - 1)
    update_columns = choice(update_cols)
    print(f'\n{update_columns}')
    if (page_range.update(rid=update_rid, columns_of_update=update_columns)):
        records_updated += 1
        updated_rids.append(update_rid)


update_time_1 = process_time()

print(f"Updating {records_updated} records took:  \t\t\t", update_time_1 - update_time_0)


print("\t\t\n\n\nReturn Record !!\n\n\n")


# updated_return_record = 0

# for rids in updated_rids:
#     updated_return_record = page_range.updated_return_record(rids)
#     print(f'\n\nRID: {updated_return_record.rid}')
#     print(f'KEY: {updated_return_record.key}')
#     print(f'GRADES: {updated_return_record.columns}')

merge_updated_list = []

@staticmethod
def __merge(page_range:Page_Range):
    print("\nMerge starting!!")
    print(f"TPS before merge: {page_range.tps_range}")
    # records that need updating in base page
    updated_records = {}

    target_page_num = 1

    # checking until what tail pages to merge
    if page_range.tps_range != 0:
        target_page_num = page_range.get_page_number(page_range.tps_range)
        print(f'Tail page to stop iterating: {page_range.get_page_number(page_range.tps_range)}')

    # iterate backwards through tail pages list
    for i in range(len(page_range.tail_pages) - 1, -1, -1):
        # Access the Tail_Page object using the current index
        tail_page = page_range.tail_pages[i]
    
        # Check if the current page number matches the target page number
        if tail_page.page_number >= target_page_num:
            print(f"MergingTail Page: {tail_page.page_number} ")
            print(f"Found tail page {tail_page.page_number}")

            # base_rid and tid page in tail page
            base_rid_page = tail_page.get_base_rid_page()
            tid_page = tail_page.physical_pages[1]
            
            # jumps straight to the end of the tail page (TODO: make it jump to very last tail record even if tail page isn't full)
            for i in range(PHYSICAL_PAGE_SIZE - COLUMN_SIZE, -1, -COLUMN_SIZE):

                tid_for_tail_record_bytes = tid_page.data[i:i+COLUMN_SIZE]

                tid_for_tail_record_value = -(int.from_bytes(tid_for_tail_record_bytes, byteorder='big', signed=True))

                # continue through loop meaning it's at a tail record that hasn't been set
                if tid_for_tail_record_value == 0:
                    continue

                # Extract 8 bytes at a time using slicing
                base_rid_for_tail_record_bytes = base_rid_page.data[i:i+COLUMN_SIZE]

                base_rid_for_tail_record_value = int.from_bytes(base_rid_for_tail_record_bytes, byteorder='big', signed=True)

                if -tid_for_tail_record_value < page_range.tps_range:
                    print(f"TPS for range : {page_range.tps_range} and TID: {-tid_for_tail_record_value}")

                    # adds rid if rid is not in update_records dictionary - used to know what base pages to use
                    if base_rid_for_tail_record_value not in updated_records.values():
                        updated_records[-(tid_for_tail_record_value)] = base_rid_for_tail_record_value

        # rids to base_pages
        rid_to_base = {}

        # base_page numbers
        base_pages_to_get = []

        for value in updated_records.values():
            base_page_num = page_range.get_page_number(value)
            rid_to_base[value] = base_page_num

            if base_page_num not in base_pages_to_get:
                base_pages_to_get.append(base_page_num)

    # sorts list
    base_pages_to_get.sort()
    print(base_pages_to_get)

    i = 0  
    # iterate through base pages in page range to find the base pages we are merging
    for base_page in page_range.base_pages:
        if i < len(base_pages_to_get) and base_page.page_number == base_pages_to_get[i] and base_page.num_records == RECORDS_PER_PAGE:
            print(f"Base page we're in : {base_page.page_number}")
            print(f"rid_to_base: {rid_to_base}")

            # keeps track of what columns were updated in this base page
            updated_columns = []

            # iterate through rids that are updated and their corresponding base page
            for key, value in rid_to_base.items():
                if value == base_page.page_number:
                    print(f"value: {value}")
                    schema_encoding_for_rid = base_page.check_base_record_schema_encoding(key)

                    # let's us know what columns have been updated
                    update_cols_for_rid = page_range.analyze_schema_encoding(schema_encoding_for_rid)

                    # grabs updated values
                    for column in update_cols_for_rid:
                        # updated values of specific columns
                        updated_val = page_range.return_column_value(key,column)

                        print(f'schema encoding: {base_page.check_base_record_schema_encoding(key)} : {update_cols_for_rid} -> [{column} : {updated_val}]  -> RID : {key} -> Page_Num {base_page.page_number}')

                        # updates column for record
                        base_page.physical_pages[META_DATA_NUM_COLUMNS + column].write_to_physical_page(value=updated_val, rid=key, update=True)

                        if column not in updated_columns:
                            updated_columns.append(column)

                        # print(f"Updated grade column ({column}) in base page {base_page.page_number} at RID: {key}\n")

                    merge_updated_list.append(key)
            
            # iterates to see what columns to read to disk if they've been modified
            for column in updated_columns:
                __merge_update_to_buffer(base_page.physical_pages[META_DATA_NUM_COLUMNS + column], base_page.page_number)
            print("Physical pages saved")

            i += 1
    
        # last record merged
        page_range.tps_range = next(iter(updated_records))
print("Merge finished")


        
            
            

    # for key, value in updated_records.items():
    #     print(f'\nDict Pair: {key} = {value}')
    #     counter_2 += 1

    # print(f"counter : {counter_2}")

# takes buffer pool that it's going to use, since merge has it's own buffer pool so it won't interfere with the main thread
@staticmethod
def __merge_update_to_buffer(physical_page:Physical_Page, base_page_number:int):
    print(f'Physical Page : {physical_page.column_number} for Base Page ({base_page_number})')

    pass


# checks if merging needs to happen
@staticmethod
def __merge_checker(page_range:Page_Range):
    print(f'amount of updates in page range: {page_range.num_updates}')
    if page_range.num_updates >= MERGE_THRESHOLD: # will use after updates in query // change to % == 0 
        print("\nMerge is initiating")
        # creates deep copy of page range
        page_range_copy = copy.deepcopy(page_range)
        merging_thread = threading.Thread(target=__merge(page_range))
        merging_thread.start()

for rids in updated_rids:
    print(f"\n\nTPS before merge: {page_range.tps_range}")
    base_return_record = page_range.return_base_record(rids)
    updated_return_record = page_range.return_record(rids)
    print(f'Base before 1st merge - RID: {base_return_record.rid}')
    print(f'Base before 1st merge - KEY: {base_return_record.key}')
    print(f'Base before 1st merge - GRADES: {base_return_record.columns}')

    print(f'Updated - RID: {updated_return_record.rid}')
    print(f'Updated - KEY: {updated_return_record.key}')
    print(f'Updated - GRADES: {updated_return_record.columns}')

__merge(page_range)

for rids in updated_rids:
    print(f"\n\nTPS after merge: {page_range.tps_range}")
    base_return_record = page_range.return_base_record(rids)
    updated_return_record = page_range.return_record(rids)
    print(f'Base after 1st merge - RID: {base_return_record.rid}')
    print(f'Base after 1st merge - KEY: {base_return_record.key}')
    print(f'Base after 1st merge - GRADES: {base_return_record.columns}')

    print(f'Updated - RID: {updated_return_record.rid}')
    print(f'Updated - KEY: {updated_return_record.key}')
    print(f'Updated - GRADES: {updated_return_record.columns}')

# print("\n\nUpdating after 1st merge!!")
# page_range.update(updated_rids[0], [None,None,62,None,None])
# page_range.update(updated_rids[0], [None,None,None,20,None])

# page_range.update(updated_rids[1], [None,None,33,None,None])
# page_range.update(updated_rids[1], [None,None,None,None,0])

# base_return_record = page_range.return_base_record(updated_rids[0])
# updated_return_record = page_range.return_record(updated_rids[0])
# print(f'Base before 2nd merge - RID: {base_return_record.rid}')
# print(f'Base before 2nd merge - KEY: {base_return_record.key}')
# print(f'Base before 2nd merge - GRADES: {base_return_record.columns}')

# print(f'Updated - RID: {updated_return_record.rid}')
# print(f'Updated - KEY: {updated_return_record.key}')
# print(f'Updated - GRADES: {updated_return_record.columns}')

print("\n\nUpdating for 2nd merge!!")
records_updated = 0
# updates record
k = 0
while records_updated < 2048:
    update_rid = updated_rids[k]
    update_columns = choice(update_cols)
    print(f'\n{update_columns}')
    if (page_range.update(rid=update_rid, columns_of_update=update_columns)):
        records_updated += 1
    k += 1

for rids in updated_rids:
    print(f"\n\nTPS before 2nd merge merge: {page_range.tps_range}")
    base_return_record = page_range.return_base_record(rids)
    updated_return_record = page_range.return_record(rids)
    print(f'Base before 2nd merge - RID: {base_return_record.rid}')
    print(f'Base before 2nd merge - KEY: {base_return_record.key}')
    print(f'Base before 2nd merge - GRADES: {base_return_record.columns}')

    print(f'Updated - RID: {updated_return_record.rid}')
    print(f'Updated - KEY: {updated_return_record.key}')
    print(f'Updated - GRADES: {updated_return_record.columns}')

__merge(page_range)

for rids in updated_rids:
    print(f"\n\nTPS after 2nd merge merge: {page_range.tps_range}")
    base_return_record = page_range.return_base_record(rids)
    updated_return_record = page_range.return_record(rids)
    print(f'Base after 2nd merge - RID: {base_return_record.rid}')
    print(f'Base after 2nd merge - KEY: {base_return_record.key}')
    print(f'Base after 2nd merge - GRADES: {base_return_record.columns}')

    print(f'Updated - RID: {updated_return_record.rid}')
    print(f'Updated - KEY: {updated_return_record.key}')
    print(f'Updated - GRADES: {updated_return_record.columns}')


print("\n\nUpdating for 3rd merge!!")
records_updated = 0
# updates record
k = 0
while records_updated < 2048:
    update_rid = updated_rids[k]
    update_columns = choice(update_cols)
    print(f'\n{update_columns}')
    if (page_range.update(rid=update_rid, columns_of_update=update_columns)):
        records_updated += 1
    k += 1

for rids in updated_rids:
    print(f"\n\nTPS before 3rd merge merge: {page_range.tps_range}")
    base_return_record = page_range.return_base_record(rids)
    updated_return_record = page_range.return_record(rids)
    print(f'Base before 3rd merge - RID: {base_return_record.rid}')
    print(f'Base before 3rd merge - KEY: {base_return_record.key}')
    print(f'Base before 3rd merge - GRADES: {base_return_record.columns}')

    print(f'Updated - RID: {updated_return_record.rid}')
    print(f'Updated - KEY: {updated_return_record.key}')
    print(f'Updated - GRADES: {updated_return_record.columns}')

__merge(page_range)

for rids in updated_rids:
    print(f"\n\nTPS after 3rd merge merge: {page_range.tps_range}")
    base_return_record = page_range.return_base_record(rids)
    updated_return_record = page_range.return_record(rids)
    print(f'Base after 3rd merge - RID: {base_return_record.rid}')
    print(f'Base after 3rd merge - KEY: {base_return_record.key}')
    print(f'Base after 3rd merge - GRADES: {base_return_record.columns}')

    print(f'Updated - RID: {updated_return_record.rid}')
    print(f'Updated - KEY: {updated_return_record.key}')
    print(f'Updated - GRADES: {updated_return_record.columns}')

# base_return_record = page_range.return_base_record(updated_rids[1])
# updated_return_record = page_range.return_record(updated_rids[1])
# print(f'Base before 2nd merge - RID: {base_return_record.rid}')
# print(f'Base before 2nd merge - KEY: {base_return_record.key}')
# print(f'Base before 2nd merge - GRADES: {base_return_record.columns}')

# print(f'Updated - RID: {updated_return_record.rid}')
# print(f'Updated - KEY: {updated_return_record.key}')
# print(f'Updated - GRADES: {updated_return_record.columns}')



# base_return_record = page_range.return_base_record(updated_rids[0])
# updated_return_record = page_range.return_record(updated_rids[0])
# print(f'Base After 2nd merge - RID: {base_return_record.rid}')
# print(f'Base After 2nd merge - KEY: {base_return_record.key}')
# print(f'Base After 2nd merge - GRADES: {base_return_record.columns}')

# print(f'Updated - RID: {updated_return_record.rid}')
# print(f'Updated - KEY: {updated_return_record.key}')
# print(f'Updated - GRADES: {updated_return_record.columns}')

# base_return_record = page_range.return_base_record(updated_rids[1])
# updated_return_record = page_range.return_record(updated_rids[1])
# print(f'Base After 2nd merge - RID: {base_return_record.rid}')
# print(f'Base After 2nd merge - KEY: {base_return_record.key}')
# print(f'Base After 2nd merge - GRADES: {base_return_record.columns}')

# print(f'Updated - RID: {updated_return_record.rid}')
# print(f'Updated - KEY: {updated_return_record.key}')
# print(f'Updated - GRADES: {updated_return_record.columns}')





