import os 
class Disk():
    def __init__(self, db_name, table_name, num_columns):
        path_name = os.getcwd() + '/' + db_name + '/' + table_name
        if not os.path.exists(path_name):
            os.makedirs(path_name)
            print("Table directory created at:", path_name)

        