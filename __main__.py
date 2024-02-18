from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
number_of_records = 10000
number_of_aggregates = 100

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []



insert_time_0 = process_time()
for i in range(0, number_of_records):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()

print(f"Inserting {number_of_records} records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, number_of_records):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print(f"Updating {number_of_records} records took:  \t\t\t", update_time_1 - update_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, number_of_records):
    query.select(choice(keys),0 , [1, 1, 1, 1, 1])
select_time_1 = process_time()
print(f"Selecting {number_of_records} records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, number_of_records, number_of_aggregates):
    start_value = 906659671 + i
    end_value = start_value + 100
    result = query.sum(start_value, end_value - 1, randrange(0, 5))
agg_time_1 = process_time()
print(f"Aggregate {number_of_records} of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, number_of_records):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print(f"Deleting {number_of_records} records took:  \t\t\t", delete_time_1 - delete_time_0)