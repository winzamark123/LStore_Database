import os 
import pickle
from lstore.table import Table
from lstore.physical_page import Physical_Page

class Disk():
    def __init__(self, db_name:str, table: Table, num_columns:int):
        self.table_name = table.name
        self.table_path = os.getcwd() + '/' + db_name + '/' + table.name
        
    def save_table_metadata(self, table:Table, path_to_disk: str) -> bool:
        metadata_path = os.path.join(path_to_disk, '_metadata.pkl')
        table_data = table.convert_table_meta_to_dict()

        try: 
            metadata_file = open(metadata_path, 'wb')
            pickle.dump(table_data, metadata_file)
            return True 
        except Exception as e:
            print("Error saving table metadata:", e)
            return False
    
    def load_table_metadata(self) -> Table:
        table_metadata_file = os.path.join(self.path_Table, '_metadata.json')
        try: 
            with open(table_metadata_file, 'r') as file:
                # table_data = json.load(file)
                # table = Table.meta_disk_to_table(table_data)
                # return table
                pass
        except Exception as e:
            print("Error loading table metadata:", e)
            return None

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
        pass

    def save_to_disk_physicalPage(self, page_range_id: int, isTail: bool, physical_page_id: int, page_to_write: Physical_Page) -> bool:
        cur_table_path = self.path_Table
        cur_page_range_path = cur_table_path + '/page_range' + str(page_range_id)
        
        if isTail:
            cur_page_range_path += '/tail/tail_page' + str(physical_page_id)
        else:
            cur_page_range_path += '/base/base_page' + str(physical_page_id)

        cur_file_path = cur_page_range_path + '/physical'

        # Ensure the directory exists before attempting to write the file
        os.makedirs(os.path.dirname(cur_file_path), exist_ok=True)

        try:
            with open(cur_file_path, 'wb') as file:
                # Assuming page_to_write.data is already a bytes object; remove .to_bytes if unnecessary
                file.write(page_to_write.data)
                print("Physical Page saved to:", cur_file_path)
                return True
        except Exception as e:
            print("Error saving physical page:", e)
            return False

    #Given the path, write the physical page into disk
    def write_to_disk(path_to_page: str, physical_page: Physical_Page) -> bool:
        try:
            with open(path_to_page, 'wb') as file:
                file.write(physical_page.data)
            return True
        except Exception as e:
            print("Error writing to disk:", e)
            return False