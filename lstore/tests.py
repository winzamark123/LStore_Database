from unittest import TestCase, main
from index import Column_Index_Tree

# NOTE: you have to start your functions w/ 'test_'
# link about it: https://www.freecodecamp.org/news/how-to-write-unit-tests-for-python-functions/#:~:text=Each%20of%20the%20tests%20that,methods%20should%20start%20with%20test_%20.

class Test_Column_Index_Tree(TestCase):
  def test_non_full_root(self):
    tree = Column_Index_Tree(5)
    self.assertTrue(len(tree) == 0)
    tree.insert_value("apple", 1)
    tree.insert_value("hello", 2)
    tree.insert_value("hi", 3)
    tree.insert_value("zucchini", 4)
    self.assertTrue(len(tree) == 4)
    self.assertTrue(tree.root.is_full() == False)
    self.assertTrue(tree.root.is_leaf == True)
    self.assertTrue(len(tree.root.child_nodes) == 0)
    self.assertTrue(tree.root.next_node == None)

  def test_full_root_split(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(1,1)
    tree.insert_value(2,2)
    self.assertTrue(tree.root.is_full() == False)
    self.assertTrue(tree.root.is_leaf == True)
    self.assertTrue(tree.root.parent == None)
    self.assertTrue(tree.root.entry_values == [1,2])
    self.assertTrue(tree.root.rids == [[1],[2]])

    tree.insert_value(3,3)
    self.assertTrue(tree.root.is_full() == False)
    self.assertTrue(tree.root.is_leaf == False)
    self.assertTrue(tree.root.parent == None)
    self.assertTrue(tree.root.entry_values == [2])
    self.assertTrue(tree.root.rids == [])
    self.assertTrue(len(tree.root.child_nodes) == 2)
    for i in range(2):
      self.assertTrue(tree.root.child_nodes[i].is_full() == False)
      self.assertTrue(tree.root.child_nodes[i].is_leaf == True)
      self.assertTrue(tree.root.child_nodes[i].parent == tree.root)
    self.assertTrue(tree.root.child_nodes[0].entry_values == [1])
    self.assertTrue(tree.root.child_nodes[0].rids == [[1]])
    self.assertTrue(tree.root.child_nodes[1].entry_values == [2,3])
    self.assertTrue(tree.root.child_nodes[1].rids == [[2],[3]])

    # next node pointers
    self.assertEqual(tree.root.child_nodes[0].next_node,
                     tree.root.child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].next_node,
                     None)

  def test_full_root(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 5):
      tree.insert_value(i,i)

    # root
    self.assertTrue(tree.root.is_full() == False)
    self.assertTrue(tree.root.is_leaf == False)
    self.assertTrue(tree.root.parent == None)
    self.assertTrue(tree.root.entry_values == [2,3])
    self.assertTrue(tree.root.rids == [])

    # leaf nodes
    self.assertTrue(len(tree.root.child_nodes) == 3)
    for i in range(3):
      self.assertTrue(tree.root.child_nodes[i].is_full() == False)
      self.assertTrue(tree.root.child_nodes[i].is_leaf == True)
      self.assertTrue(tree.root.child_nodes[i].parent == tree.root)
    self.assertTrue(tree.root.child_nodes[0].entry_values == [1])
    self.assertTrue(tree.root.child_nodes[0].rids == [[1]])
    self.assertTrue(tree.root.child_nodes[1].entry_values == [2])
    self.assertTrue(tree.root.child_nodes[1].rids == [[2]])
    self.assertTrue(tree.root.child_nodes[2].entry_values == [3,4])
    self.assertTrue(tree.root.child_nodes[2].rids == [[3],[4]])

    # next node pointers
    self.assertEqual(tree.root.child_nodes[0].next_node,
                     tree.root.child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].next_node,
                     tree.root.child_nodes[2])
    self.assertEqual(tree.root.child_nodes[2].next_node,
                     None)

  def test_one_inner_level(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 6):
      tree.insert_value(i,i)

    # root
    self.assertTrue(tree.root.is_leaf == False)
    self.assertTrue(tree.root.entry_values == [3])
    self.assertTrue(tree.root.rids == [])
    self.assertTrue(tree.root.parent == None)

    # inner level
    self.assertTrue(len(tree.root.child_nodes) == 2)
    for i in range(2):
      self.assertTrue(tree.root.child_nodes[i].is_leaf == False)
      self.assertTrue(tree.root.child_nodes[i].parent == tree.root)
      self.assertTrue(tree.root.child_nodes[i].rids == [])
    self.assertTrue(tree.root.child_nodes[0].entry_values == [2])
    self.assertTrue(tree.root.child_nodes[1].entry_values == [4])

    # leaf nodes
    self.assertTrue(len(tree.root.child_nodes[0].child_nodes) == 2)
    self.assertTrue(len(tree.root.child_nodes[1].child_nodes) == 2)
    for i in range(2):
      for j in range(2):
        self.assertTrue(tree.root.child_nodes[i].child_nodes[j].is_leaf == True)
        self.assertEqual(tree.root.child_nodes[i].child_nodes[j].parent,
                        tree.root.child_nodes[i])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[0].entry_values == [1])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[0].rids == [[1]])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[1].entry_values == [2])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[1].rids == [[2]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].entry_values == [3])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].rids == [[3]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].entry_values == [4,5])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].rids == [[4],[5]])

    # next node pointers
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].next_node,
                     tree.root.child_nodes[0].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].next_node,
                     tree.root.child_nodes[1].child_nodes[0])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].next_node,
                     tree.root.child_nodes[1].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].next_node,
                     None)

    # test 2nd value appended to inner node array
    tree.insert_value(6,6)
    self.assertTrue(tree.root.child_nodes[1].is_leaf == False)
    self.assertTrue(tree.root.child_nodes[1].parent == tree.root)
    self.assertTrue(tree.root.child_nodes[1].entry_values == [4,5])

    # leaf nodes after 2nd value to inner
    self.assertTrue(len(tree.root.child_nodes[1].child_nodes) == 3)
    for i in range(3):
      self.assertTrue(tree.root.child_nodes[1].child_nodes[i].is_leaf == True)
      self.assertEqual(tree.root.child_nodes[1].child_nodes[i].parent,
                       tree.root.child_nodes[1])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].entry_values == [3])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].rids == [[3]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].entry_values == [4])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].rids == [[4]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[2].entry_values == [5,6])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[2].rids == [[5],[6]])

    # more next node pointers
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].next_node,
                     tree.root.child_nodes[1].child_nodes[0])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].next_node,
                     tree.root.child_nodes[1].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].next_node,
                     tree.root.child_nodes[1].child_nodes[2])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[2].next_node, None)

    # test 2nd value appended to root node array
    tree.insert_value(7,7)
    self.assertTrue(tree.root.is_leaf == False)
    self.assertTrue(tree.root.parent == None)
    self.assertTrue(tree.root.entry_values == [3,5])

    # inner nodes after 2nd value to root
    self.assertTrue(len(tree.root.child_nodes) == 3)
    for i in range(3):
      self.assertTrue(tree.root.child_nodes[i].is_leaf == False)
      self.assertTrue(tree.root.child_nodes[i].parent == tree.root)
      self.assertTrue(tree.root.child_nodes[i].rids == [])
      self.assertTrue(tree.root.child_nodes[0].entry_values == [2])
      self.assertTrue(tree.root.child_nodes[1].entry_values == [4])
      self.assertTrue(tree.root.child_nodes[2].entry_values == [6])

    # leaf nodes after 2nd value to root
    for i, j in [(0,2),(1,2),(2,2)]:
      self.assertTrue(len(tree.root.child_nodes[i].child_nodes) == j)
    for i in range(3):
      for j in range(2):
        self.assertTrue(tree.root.child_nodes[i].child_nodes[j].is_leaf == True)
    self.assertTrue(tree.root.child_nodes[0].child_nodes[0].entry_values == [1])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[0].rids == [[1]])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[1].entry_values == [2])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[1].rids == [[2]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].entry_values == [3])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].rids == [[3]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].entry_values == [4])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].rids == [[4]])
    self.assertTrue(tree.root.child_nodes[2].child_nodes[0].entry_values == [5])
    self.assertTrue(tree.root.child_nodes[2].child_nodes[0].rids == [[5]])
    self.assertTrue(tree.root.child_nodes[2].child_nodes[1].entry_values == [6,7])
    self.assertTrue(tree.root.child_nodes[2].child_nodes[1].rids == [[6],[7]])

    # even more node pointers
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].next_node,
                     tree.root.child_nodes[0].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].next_node,
                     tree.root.child_nodes[1].child_nodes[0])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].next_node,
                     tree.root.child_nodes[1].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].next_node,
                     tree.root.child_nodes[2].child_nodes[0])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].next_node,
                     tree.root.child_nodes[2].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[1].next_node,
                     None)

  def test_two_inner_levels(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 10):
      tree.insert_value(i,i)

    # root
    self.assertTrue(tree.root.entry_values == [5])
    # TODO: continue working on this test

  def test_equality(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 101):
      tree.insert_value(i,i)

    for i in range(1, 101):
      self.assertTrue(tree.get_rids_equality_search(i) == [i])

  def test_random_equality(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(1,1)
    self.assertTrue(tree.get_rids_equality_search(1) == [1])
    tree.insert_value(1,2)
    self.assertTrue(tree.get_rids_equality_search(1) == [1,2])
    tree.insert_value(2,3)
    self.assertTrue(tree.get_rids_equality_search(2) == [3])

    # split tree
    tree.insert_value(3, 4)
    self.assertTrue(tree.get_rids_equality_search(1) == [1,2])

    # make sure that tree looks good
    self.assertTrue(tree.root.entry_values == [2])
    self.assertTrue(tree.root.child_nodes[0].entry_values == [1])
    self.assertTrue(tree.root.child_nodes[1].entry_values == [2,3])
    for i in range(2):
      self.assertTrue(tree.root.child_nodes[i].is_leaf)

    # check RIDs of leaf nodes
    self.assertTrue(tree.root.child_nodes[0].rids == [[1,2]])
    self.assertTrue(tree.root.child_nodes[1].rids == [[3],[4]])

    # get RID of middle entry value
    self.assertTrue(tree.get_rids_equality_search(2) == [3])

    # get RID of last entry value
    self.assertTrue(tree.get_rids_equality_search(3) == [4])

  def test_range_rids(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(1,1)
    tree.insert_value(1,2)
    tree.insert_value(2,3)
    tree.insert_value(3,4)
    tree.insert_value(0,5)

    # check to see if tree is correct
    self.assertTrue(tree.root.entry_values == [2])
    self.assertTrue(tree.root.child_nodes[0].entry_values == [0,1])
    self.assertTrue(tree.root.child_nodes[1].entry_values == [2,3])

    # get RID range from same inner node
    self.assertTrue(tree.get_rids_range_search(0,1) == [5,1,2])

    # get RID range between two inner nodes
    self.assertTrue(tree.get_rids_range_search(0,2) == [5,1,2,3])

  def test_get_all_rids(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 6):
      tree.insert_value(i,i)

    # make sure nodes are proper is_leaf boolean
    self.assertTrue(tree.root.is_leaf == False)
    for i in range(2):
      self.assertTrue(tree.root.child_nodes[i].is_leaf == False)
      for j in range(2):
        self.assertTrue(tree.root.child_nodes[i].child_nodes[j].is_leaf == True)

    # make sure leaf nodes have RIDs
    self.assertTrue(tree.root.child_nodes[0].child_nodes[0].rids == [[1]])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[1].rids == [[2]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].rids == [[3]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].rids == [[4],[5]])

    # test get range
    self.assertTrue(tree.get_rids_range_search(1,5) == [1,2,3,4,5])

  def test_large_dataset(self):
    tree = Column_Index_Tree(4)
    for i in range(1,10001):
      tree.insert_value(10000 - i,i)
    
    for i in range(1,10001):
      self.assertTrue(tree.get_rids_equality_search(10000 - i) == [i])
    self.assertTrue(tree.get_rids_range_search(1,10001) == [i for i in range(1,10001)])

if __name__ == "__main__":
  main()
