import os 
import json
from lstore.table import Table
from lstore.physical_page import Physical_Page

class Disk():
    def __init__(self, db_name:str, table_name:str, num_columns:int):
        self.path_name = os.getcwd() + '/' + db_name + '/' + table_name # /*/ECS165/Grades

        if not os.path.exists(self.path_name):
            os.makedirs(self.path_name)
            print("Table directory created at:", self.path_name)
        
        for column_index in range(num_columns):
            file_name = self.path_name + '/' + str(column_index)
            if not os.path.exists(file_name):
                file = open(file_name, 'wb')

                # empty_page = Page(0, [0], 0)
                # file.write((0).to_bytes(4, byteorder='big'))
                # file.write((empty_page.num_records).page_to_bytes())
                # file.write(empty_page.physical_pages.to_bytes())
    
    def save_table_metadata(self, table:Table) -> bool:
        table_data = table.meta_table_to_disk()
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
                table = Table.meta_disk_to_table(table_data)
                return table
        except Exception as e:
            print("Error loading table metadata:", e)
            return None

    def load_page(self, col_num: int) -> Physical_Page:
        page_num_file = os.path.join(self.path_name, str(col_num))
        try:
            with open(page_num_file, 'rb') as file:
                num_records = int.from_bytes(file.read(4), byteorder='big')
                physical_pages = int.from_bytes(file.read(4), byteorder='big')
                page = Physical_Page(col_num, [0], num_records)
                page.physical_pages = physical_pages
                return page
        except Exception as e:
            print("Error loading page:", e)
            return None
        pass
        
    def save_page(self, physical_page: Physical_Page) -> bool:
        pass   

        