from lstore.page import *
from lstore.record import Record
from random import randint
from time import process_time

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

# creates base page list
base_pages = []

def add_base_page(num_columns, entry_sizes, key_column):
    base_page = Base_Page(num_columns=num_columns, entry_sizes=entry_sizes, key_column=key)
    base_pages.append(base_page)

# Create the initial base page
add_base_page(num_columns, entry_size_for_columns, key)

def check_and_add_base_page():
    # checks the last base page in the base page list to see if it's full
    if base_pages[-1].page_is_full(): 
        add_base_page(num_columns=num_columns, entry_sizes=entry_size_for_columns, key_column=key)

print(f"Base Page {base_pages[0].page_number}")


# Initialize a counter for records (RIDs)
record_counter = 1

# Initialize the index of the current base page
current_base_page_index = 0

"""
INSERT RECORDS
"""

insert_time_0 = process_time()
# Loop to keep adding records
while True:
    # Insert a new record into the current base page
    if base_pages[current_base_page_index].insert_new_record(Record(record_counter, 906659672 + record_counter, (randint(1,20), randint(1,20), randint(1,20), randint(1,20)))):
        print(f"Inserted record with RID {record_counter} into Base Page {current_base_page_index + 1}\n")
        if (record_counter == 10000):
           break
        record_counter += 1
    else:
        # If the current base page is full, move to the next base page
        current_base_page_index += 1

        # stop creating base pages after 3 base pages for this example
        #if current_base_page_index == 1:
            #break
        
        # Check if the current base page index exceeds the number of base pages
        if current_base_page_index >= len(base_pages):
            # If so, add a new base page to the list
            print(f"Base Page {current_base_page_index} is full. Adding a new base page.")
            add_base_page(num_columns, entry_size_for_columns, key)

insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)


"""
UPDATE A CERTAIN RECORD

print("\n\nUpdating Record!!")

# Student ID that's going to update , random Student ID for this testing
stID = 906659672 + randint(1,((512) * current_base_page_index))

# Grade (1-4) they want to update
grade_to_update = randint(1,4) 

# new value for grade_to_update
new_grade = randint(1,20)

# "RID" for tail pages
lid = 0

# Iterate through each base page in list
for base_page in base_pages:
    try:
        # Get the RID associated with the student ID from the current base page
        rid_for_student = base_page.get_rid_for_key(stID)

        indirection_page = base_page.get_indirection_page()

        

        print(f'\nStudent ID ({stID}) is associated with RID: ({rid_for_student}) in Base Page {base_page.page_number} associated with tail page {base_page.tail_page.page_number}')
        print(f'\nValue before Update for column ({indirection_page.column_number}): ({indirection_page.value_exists_at_bytes(rid_for_student)})\n')

        lid -= 1
        if(base_page.tail_page.value_getting_updated_tail_page(grade_to_update,new_grade, rid_for_student ,lid)):
            print(f"Added to tail page {base_page.tail_page.page_number}")


        # update column (physical page) with new grade
        if(base_page.updating_base_record(stID, rid_for_student, lid)):
            print("Value was updated")
            print(f'\nValue after the Update for column ({indirection_page.column_number}): ({indirection_page.value_exists_at_bytes(rid_for_student)})\n')
            
        break  # Exit loop if the student ID is found in any base page
    except KeyError:
        # If the student ID is not found in the current base page, continue searching in the next base page
        continue
else:
    # If the loop completes without finding the student ID in any base page, print a message
    print(f'Student ID ({stID}) is not found in any base page.')

"""