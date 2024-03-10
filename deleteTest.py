from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open("./ECS165")
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table("Grades", 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 10

seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")

# Check inserted records using select query
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print("select error on", key, ":", record, ", correct:", records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

print("NUMBER OF RECORDS", grades_table.num_records)
print("First Record", query.select(92106429, 0, [1, 1, 1, 1, 1])[0].columns)

# DELETE TEST
for i in range(0, number_of_records):
    query.delete(92106429 + i)

print("Delete finished")
print("NUMBER OF RECORDS", grades_table.num_records)
print("First Record", query.select(92106429, 0, [1, 1, 1, 1, 1])[0].columns)

for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print("select error on", key, ":", record, ", correct:", records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

db.close()
