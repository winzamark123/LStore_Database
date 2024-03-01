from shutil import rmtree
from index import Index
from unittest import TestCase, main
from random import seed, randint

def rm_index(index:Index):
  index.__del__()
  rmtree("index")

class TestBPTree(TestCase):

  def test_add_invalid_record(self):
    index = Index("test_table_1", ["Bio", "Chem"], 0, 4)
    try:
      index.insert_record_to_index([1], 1)
    except ValueError:
      self.assertTrue(True)
    else:
      self.assertTrue(False)
    rm_index(index)

  def test_add_valid_record(self):
    index = Index("test_table_2", ["Bio", "Chem"], 1, 4)
    index.insert_record_to_index([12, 13], 1)
    self.assertEqual(index.locate(12, 0), {1})
    rm_index(index)

  def test_add_duplicate_record_entry_values(self):
    index = Index("test_table_3", ["Bio", "Chem"], 1, 20)
    index.insert_record_to_index([12, 13], 1)
    index.insert_record_to_index([12, 14], 2)
    self.assertEqual(index.locate(12, 0), {1,2})
    rm_index(index)

  def test_locate_range(self):
    index = Index("test_table_4", ["Bio", "Chem"], 1, 20)
    tot_vals = set()
    spec_vals = set()
    for i in range(1, 101):
      val = randint(0, 100)
      tot_vals.add(i)
      if 50 <= val and val <= 68:
        spec_vals.add(i)
      index.insert_record_to_index([val, i], i)
    self.assertEqual(index.locate_range(0, 100, 0), tot_vals)
    self.assertEqual(index.locate_range(50, 68, 0), spec_vals)
    rm_index(index)

  def test_add_multiple_records(self):
    seed(100)
    index = Index("test_table_5", ["SID", "Bio", "Chem", "CS", "Sociology"], 0, 20)
    records_dict = dict()
    for _ in range(1, 21):
      SID = randint(1, 1000)
      while SID in records_dict:
        SID = randint(1, 1000)
      records_dict[SID] = [randint(0,100), randint(0,100), randint(0,100), randint(0,100)]
    for i, key in enumerate(records_dict):
      index.insert_record_to_index([key] + records_dict[key], i+1)
    for i, (key, values) in enumerate(records_dict.items()):
      self.assertEqual(index.locate(key, 0), {i+1})
      for j, value in enumerate(values):
        self.assertIn(i+1, index.locate(value, j+1))
    rm_index(index)
  
  def test_update_entry(self):
    index = Index("test_table_6", ["SID", "Bio", "Chem", "CS"], 0, 20)
    index.insert_record_to_index([10, 90, 20, 30], 1)
    index.insert_record_to_index([11, 20, 30, 21], 2)
    index.update_entry(90, 0, 1, 1)
    index.update_entry(30, 20, 2, 2)
    self.assertEqual(index.locate(0, 1), {1})
    self.assertEqual(index.locate(20, 2), {1,2})
    rm_index(index)

  def test_create_index(self):
    index = Index("test_table_7", ["SID"], 0, 20)
    rm_index(index)


if __name__ == "__main__":
  main()