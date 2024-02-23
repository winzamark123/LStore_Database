import lstore.page_range as Page_Range
import lstore.physical_page as Physical_Page
class Frame:
    def __init__(self, physical_page: Physical_Page):
        self.physical_page = physical_page # Page object
        self.dirty = 0 # Boolean to check if the physical_page has been modified
        self.pin_count = 0

    def get_pin_count(self) -> int:
        return self.pin_count
    
    def is_dirty(self) -> bool:
        return self.dirty

    
