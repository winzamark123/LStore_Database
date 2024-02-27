import os 
import pickle
from lstore.physical_page import Physical_Page

class Disk:
    def __init__(self, db_name:str, table_name:str, num_columns:int):
        self.table_name = table_name
        self.table_path = os.getcwd() + '/' + db_name + '/' + table_name 

        if not os.path.exists(self.table_path):
            os.makedirs(self.table_path)
            print("Table directory created at:", self.table_path)
        
    def write_metadata_to_disk(self, path_to_disk: str, meta_data: dict) -> bool:
        metadata_path = os.path.join(path_to_disk, '_metadata.pkl')

        try: 
            metadata_file = open(metadata_path, 'wb')
            pickle.dump(meta_data, metadata_file)
            return True 
        except Exception as e:
            print("Error saving table metadata:", e)
            return False
    
    # def load_table_metadata(self) -> Table:
    #     table_metadata_file = os.path.join(self.path_Table, '_metadata.json')
    #     try: 
    #         with open(table_metadata_file, 'r') as file:
    #             # table_data = json.load(file)
    #             # table = Table.meta_disk_to_table(table_data)
    #             # return table
    #             pass
    #     except Exception as e:
    #         print("Error loading table metadata:", e)
    #         return None

    def load_page(self, col_num: int) -> Physical_Page:
        page_num_file = os.path.join(self.path_Table, str(col_num))
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
