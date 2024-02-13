class Record:

    def __init__(self, rid:int, key:int, columns:tuple)->None:
        self.rid = rid
        self.key = key
        self.columns = columns
    
    def get_key(self):
        return self.key
    
    def get_values(self):
        return (self.key,) + tuple(self.columns)