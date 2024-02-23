import lstore.page_range as Page_Range
import lstore.page as Page
class Frame:
    def __init__(self, page: Page):
        self.page = page # Page object
        self.dirty = 0 # Boolean to check if the page has been modified
        self.pin_count = 0

    
