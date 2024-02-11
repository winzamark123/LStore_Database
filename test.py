from lstore.page import *
from lstore.record import Record
from random import randint
from time import process_time
from page_range_2 import *

# To make it work, uncomment all the print() statements in page.py to see the physical pages get updated 

# Replace with your actual number of columns
num_columns = 5 

# key column 
key = 0

# These first 3 are for meta columns
entry_size_for_columns = [2,COLUMN_SIZE,COLUMN_SIZE]

# adds to list depending on how many columns are asked from user
for i in range(num_columns): 
    entry_size_for_columns.append(COLUMN_SIZE)

page_range = Page_Range(num_columns, entry_size_for_columns, key)
print('Base Page: ',page_range.base_pages[0].page_number)
print('Tail Page: ', page_range.tail_pages[0].page_number)

#page_range.update_base_page(51, [None,1,None,None,None])



# Initialize a counter for records (RIDs)
record_counter = 1

# Initialize the index of the current base page
current_base_page_index = 0

"""
#INSERT RECORDS
"""

insert_time_0 = process_time()
# Loop to keep adding records
while True:
    # Insert a new record into the current base page
    if page_range.base_pages[current_base_page_index].insert_new_record(Record(record_counter, 906659672 + record_counter, (randint(1,20), randint(1,20), randint(1,20), randint(1,20)))):
        print(f"Inserted record with RID {record_counter} into Base Page {current_base_page_index + 1}\n")
        #if (record_counter == 10000):
           #break
        record_counter += 1
    else:
        # If the current base page is full, move to the next base page
        current_base_page_index += 1

        # stop creating base pages after 3 base pages for this example
        if current_base_page_index == 5:
            break
        
        # Check if the current base page index exceeds the number of base pages
        if current_base_page_index >= len(page_range.base_pages):
            # If so, add a new base page to the list
            print(f"Base Page {current_base_page_index} is full. Adding a new base page.")
            page_range.insert_base_page()

insert_time_1 = process_time()

print("Inserting records into 5 base pages took:  \t\t\t", insert_time_1 - insert_time_0)

page_range.update(510,[None,None,6,None,None])

"""
"""
#UPDATE A CERTAIN RECORD
"""

print("\n\nUpdating Record!!")

# Student ID that's going to update , random Student ID for this testing
upgrade_rid = randint(1,((512) * current_base_page_index))

# Grade (1-4) they want to update
grade_to_update = randint(1,4) 

# new value for grade_to_update
new_grade = randint(1,20)

# "RID" for tail pages
lid = 0


insert_time_2 = process_time()
# Iterate through each base page in list
for base_page in page_range.base_pages:
    try:
        if(base_page.check_for_rid(upgrade_rid)):
                indirection_page = base_page.get_indirection_page()


                print(f'\nRID: ({upgrade_rid}) in Base Page {base_page.page_number}')
                print(f'\nValue before Update for column ({indirection_page.column_number}): ({base_page.check_base_record_indirection(upgrade_rid)})\n')

                lid -= 1
                # update column (physical page) with new grade
                if(base_page.update_indirection_base_column(lid, upgrade_rid)):
                    print("Value was updated")
                    print(f'\nValue after the Update for column ({indirection_page.column_number}): ({base_page.check_base_record_indirection(upgrade_rid)})\n')
                    
                break  # Exit loop if the student ID is found in any base page
    except KeyError:
        # If the student ID is not found in the current base page, continue searching in the next base page
        continue
else:
    # If the loop completes without finding the student ID in any base page, print a message
    print(f'RID ({upgrade_rid}) is not found in any base page.')
insert_time_3 = process_time()

print("Updating 1 column in a record took:  \t\t\t", insert_time_1 - insert_time_0)
"""