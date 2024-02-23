import os 
import json
from lstore.table import Table

class Disk():
    def __init__(self, db_name:str, table_name:str, num_columns:int):
        path_name = os.getcwd() + '/' + db_name + '/' + table_name # /*/ECS165/Grades

        if not os.path.exists(path_name):
            os.makedirs(path_name)
            print("Table directory created at:", path_name)

    
    def save_table_metadata(self, table:Table) -> bool:
        table_data = table.table_to_disk()
        table_file = os.path.join(self.path_name, '_metadata.json')
        try: 
            with open(table_file, 'w') as file:
                json.dump(table_data, file)
            print("Table metadata saved to:", table_file)
            return True 
        except Exception as e:
            print("Error saving table metadata:", e)
            return False
    
    def load_table_metadata(self) -> Table:
        table_metadata_file = os.path.join(self.path_name, '_metadata.json')
        try: 
            with open(table_metadata_file, 'r') as file:
                table_data = json.load(file)
                table = Table.disk_to_table(table_data)
                return table
        except Exception as e:
            print("Error loading table metadata:", e)
            return None

        

        