from unittest import TestCase, main
from random import choice, randint, sample, seed
from old_bptree_index import Index, Column_Index_Tree, Column_Index_Node
import sys
import io

# NOTE: you have to start your functions w/ 'test_'
# link about it: https://www.freecodecamp.org/news/how-to-write-unit-tests-for-python-functions/#:~:text=Each%20of%20the%20tests%20that,methods%20should%20start%20with%20test_%20.

class Test_Column_Index_Tree(TestCase):

  def test_non_full_root(self):
    tree = Column_Index_Tree(5)
    self.assertEqual(len(tree), 0)
    tree.insert_value("apple", 1)
    tree.insert_value("hello", 2)
    tree.insert_value("hi", 3)
    tree.insert_value("zucchini", 4)
    self.assertEqual(len(tree), 4)
    self.assertEqual(tree.root.is_full(), False)
    self.assertEqual(tree.root.is_leaf, True)
    self.assertEqual(len(tree.root.child_nodes), 0)
    self.assertEqual(tree.root.next_node, None)

  def test_full_root_split(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(1,1)
    tree.insert_value(2,2)
    self.assertEqual(tree.root.is_full(), False)
    self.assertEqual(tree.root.is_leaf, True)
    self.assertEqual(tree.root.parent, None)
    self.assertEqual(tree.root.get_keys(), [1,2])
    self.assertEqual(tree.root.get_rids(), [{1}, {2}])

    tree.insert_value(3,3)
    self.assertEqual(tree.root.is_full(), False)
    self.assertEqual(tree.root.is_leaf, False)
    self.assertEqual(tree.root.parent, None)
    self.assertEqual(tree.root.get_keys(), [2])
    self.assertEqual(tree.root.get_rids(), [])
    self.assertEqual(len(tree.root.child_nodes), 2)
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[i].is_full(), False)
      self.assertEqual(tree.root.child_nodes[i].is_leaf, True)
      self.assertEqual(tree.root.child_nodes[i].parent, tree.root)
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [1])
    self.assertEqual(tree.root.child_nodes[0].get_rids(), [{1}])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [2,3])
    self.assertEqual(tree.root.child_nodes[1].get_rids(), [{2},{3}])

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
    self.assertEqual(tree.root.is_full(), False)
    self.assertEqual(tree.root.is_leaf, False)
    self.assertEqual(tree.root.parent, None)
    self.assertEqual(tree.root.get_keys(), [2,3])
    self.assertEqual(tree.root.get_rids(), [])

    # leaf nodes
    self.assertEqual(len(tree.root.child_nodes), 3)
    for i in range(3):
      self.assertEqual(tree.root.child_nodes[i].is_full(), False)
      self.assertEqual(tree.root.child_nodes[i].is_leaf, True)
      self.assertEqual(tree.root.child_nodes[i].parent, tree.root)
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [1])
    self.assertEqual(tree.root.child_nodes[0].get_rids(), [{1}])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [2])
    self.assertEqual(tree.root.child_nodes[1].get_rids(), [{2}])
    self.assertEqual(tree.root.child_nodes[2].get_keys(), [3,4])
    self.assertEqual(tree.root.child_nodes[2].get_rids(), [{3},{4}])

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
    self.assertEqual(tree.root.is_leaf, False)
    self.assertEqual(tree.root.get_keys(), [3])
    self.assertEqual(tree.root.get_rids(), [])
    self.assertEqual(tree.root.parent, None)

    # inner level
    self.assertEqual(len(tree.root.child_nodes), 2)
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[i].is_leaf, False)
      self.assertEqual(tree.root.child_nodes[i].parent, tree.root)
      self.assertEqual(tree.root.child_nodes[i].next_node, None)
      self.assertEqual(tree.root.child_nodes[i].get_rids(), [])
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [2])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [4])

    # leaf nodes
    self.assertEqual(len(tree.root.child_nodes[0].child_nodes), 2)
    self.assertEqual(len(tree.root.child_nodes[1].child_nodes), 2)
    for i in range(2):
      for j in range(2):
        self.assertEqual(tree.root.child_nodes[i].child_nodes[j].is_leaf, True)
        self.assertEqual(tree.root.child_nodes[i].child_nodes[j].parent,
                        tree.root.child_nodes[i])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].get_keys(), [1])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].get_rids(), [{1}])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_keys(), [2])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_rids(), [{2}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_keys(), [3])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_rids(), [{3}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [4,5])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_rids(), [{4},{5}])

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
    self.assertEqual(tree.root.child_nodes[1].is_leaf, False)
    self.assertEqual(tree.root.child_nodes[1].parent, tree.root)
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [4,5])
    self.assertEqual(tree.root.child_nodes[1].get_rids(), [])

    # leaf nodes after 2nd value to inner
    self.assertEqual(len(tree.root.child_nodes[1].child_nodes), 3)
    for i in range(3):
      self.assertEqual(tree.root.child_nodes[1].child_nodes[i].is_leaf, True)
      self.assertEqual(tree.root.child_nodes[1].child_nodes[i].parent,
                       tree.root.child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_keys(), [3])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_rids(), [{3}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [4])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_rids(), [{4}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[2].get_keys(), [5,6])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[2].get_rids(), [{5},{6}])

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
    self.assertEqual(tree.root.is_leaf, False)
    self.assertEqual(tree.root.parent, None)
    self.assertEqual(tree.root.get_keys(), [3,5])
    self.assertEqual(tree.root.get_rids(), [])

    # inner nodes after 2nd value to root
    self.assertEqual(len(tree.root.child_nodes), 3)
    for i in range(3):
      self.assertEqual(tree.root.child_nodes[i].is_leaf, False)
      self.assertEqual(tree.root.child_nodes[i].parent, tree.root)
      self.assertEqual(tree.root.child_nodes[i].get_rids(), [])
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [2])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [4])
    self.assertEqual(tree.root.child_nodes[2].get_keys(), [6])

    # leaf nodes after 2nd value to root
    for i, j in [(0,2),(1,2),(2,2)]:
      self.assertEqual(len(tree.root.child_nodes[i].child_nodes), j)
    for i in range(3):
      for j in range(2):
        self.assertEqual(tree.root.child_nodes[i].child_nodes[j].is_leaf, True)
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].get_keys(), [1])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].get_rids(), [{1}])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_keys(), [2])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_rids(), [{2}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_keys(), [3])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_rids(), [{3}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [4])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_rids(), [{4}])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [5])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_rids(), [{5}])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[1].get_keys(), [6,7])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[1].get_rids(), [{6},{7}])

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
    self.assertEqual(tree.root.is_leaf, False)
    self.assertEqual(tree.root.parent, None)
    self.assertEqual(tree.root.get_keys(), [5])
    self.assertEqual(len(tree.root.child_nodes), 2)

    # 1st lvl inner nodes
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[i].is_leaf, False)
      self.assertEqual(tree.root.child_nodes[i].parent, tree.root)
      self.assertEqual(tree.root.child_nodes[i].next_node, None)
      self.assertEqual(tree.root.child_nodes[i].get_rids(), [])
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [3])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [7])

    # 2nd lvl inner nodes
    ## 0th inner node
    self.assertEqual(len(tree.root.child_nodes[0].child_nodes), 2)
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[0].child_nodes[i].is_leaf, False)
      self.assertEqual(tree.root.child_nodes[0].child_nodes[i].parent, tree.root.child_nodes[0])
      self.assertEqual(tree.root.child_nodes[0].child_nodes[i].next_node, None)
      self.assertEqual(tree.root.child_nodes[0].child_nodes[i].get_rids(), [])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].get_keys(), [2])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_keys(), [4])
    ## 1st inner node
    self.assertEqual(len(tree.root.child_nodes[1].child_nodes), 2)
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[1].child_nodes[i].is_leaf, False)
      self.assertEqual(tree.root.child_nodes[1].child_nodes[i].parent, tree.root.child_nodes[1])
      self.assertEqual(tree.root.child_nodes[1].child_nodes[i].next_node, None)
      self.assertEqual(tree.root.child_nodes[1].child_nodes[i].get_rids(), [])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_keys(), [6])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [8])

    # leaf nodes (only left-most ones)
    self.assertEqual(len(tree.root.child_nodes[1].child_nodes[1].child_nodes), 2)
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[1].child_nodes[1].child_nodes[i].is_leaf, True)
      self.assertEqual(tree.root.child_nodes[1].child_nodes[1].child_nodes[i].parent,
                       tree.root.child_nodes[1].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].child_nodes[0].get_keys(), [7])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].child_nodes[0].get_rids(), [{7}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].child_nodes[1].get_keys(), [8,9])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].child_nodes[1].get_rids(), [{8},{9}])

    # check next node pointers
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].child_nodes[1].next_node,
                     tree.root.child_nodes[1].child_nodes[1].child_nodes[0])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].child_nodes[0].next_node,
                    tree.root.child_nodes[1].child_nodes[1].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].child_nodes[1].next_node, None)

  def test_reversed_insert_non_full(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(2,1)
    tree.insert_value(1,2)

    self.assertEqual(tree.root.is_leaf, True)
    self.assertEqual(tree.root.next_node, None)
    self.assertEqual(tree.root.get_rids(), [{2},{1}])

  def test_reversed_insert_split(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(4,1)
    tree.insert_value(3,2)
    tree.insert_value(2,3)
    tree.insert_value(1,4)

    # root
    self.assertEqual(tree.root.is_leaf, False)
    self.assertEqual(tree.root.next_node, None)
    self.assertEqual(tree.root.get_rids(), [])
    self.assertEqual(tree.root.get_keys(), [3])

    # leaf nodes
    self.assertEqual(len(tree.root.child_nodes), 2)
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[i].is_leaf, True)
      self.assertEqual(tree.root.child_nodes[i].parent, tree.root)
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [1,2])
    self.assertTrue(tree.root.child_nodes[0].get_rids() ==[{4},{3}])
    self.assertEqual(tree.root.child_nodes[0].next_node,
                     tree.root.child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [3,4])
    self.assertEqual(tree.root.child_nodes[1].get_rids(), [{2},{1}])
    self.assertEqual(tree.root.child_nodes[1].next_node, None)

    # 1st extra leaf node insert
    tree.insert_value(0,5)
    self.assertEqual(len(tree.root.child_nodes), 3)
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [0])
    self.assertEqual(tree.root.child_nodes[0].get_rids(), [{5}])
    self.assertEqual(tree.root.child_nodes[0].next_node,
                     tree.root.child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [1,2])
    self.assertEqual(tree.root.child_nodes[1].get_rids(), [{4},{3}])
    self.assertEqual(tree.root.child_nodes[1].next_node,
                     tree.root.child_nodes[2])
    self.assertEqual(tree.root.child_nodes[2].get_keys(), [3,4])
    self.assertEqual(tree.root.child_nodes[2].get_rids(), [{2},{1}])
    self.assertNotEqual(tree.root.child_nodes[2].next_node, tree.root.child_nodes[0])
    self.assertEqual(tree.root.child_nodes[2].next_node, None)

  def test_reversed_insert_one_inner_node_last_leaf_node_is_none(self):
    tree = Column_Index_Tree(3)
    for i in range(1,8):
      tree.insert_value(8-i,i)

    self.assertEqual(tree.root.get_keys(), [4])

    self.assertEqual(len(tree.root.child_nodes), 2)

    self.assertEqual(len(tree.root.child_nodes[1].child_nodes), 2)
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [6,7])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].next_node, None)

  def test_reversed_insert_two_inner_nodes_last_leaf_node_is_none(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 10):
      tree.insert_value(10-i,i)

    # root
    self.assertEqual(tree.root.get_keys(), [6])

    # last node pointer
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [8,9])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].next_node, None)

  def test_equality(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 101):
      tree.insert_value(i,i)

    for i in range(1, 101):
      self.assertEqual(tree.get_rids_equality_search(i), {i})

  def test_random_equality(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(1,1)
    self.assertEqual(tree.get_rids_equality_search(1), {1})
    tree.insert_value(1,2)
    self.assertEqual(tree.get_rids_equality_search(1), {1,2})
    tree.insert_value(2,3)
    self.assertEqual(tree.get_rids_equality_search(2), {3})

    # split tree
    tree.insert_value(3, 4)
    self.assertEqual(tree.get_rids_equality_search(1), {1,2})

    # make sure that tree looks good
    self.assertEqual(tree.root.get_keys(), [2])
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [1])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [2,3])
    for i in range(2):
      self.assertTrue(tree.root.child_nodes[i].is_leaf)

    # check RIDs of leaf nodes
    self.assertEqual(tree.root.child_nodes[0].get_rids(), [{1,2}])
    self.assertEqual(tree.root.child_nodes[1].get_rids(), [{3},{4}])

    # get RID of middle entry value
    self.assertEqual(tree.get_rids_equality_search(2), {3})

    # get RID of last entry value
    self.assertEqual(tree.get_rids_equality_search(3), {4})

  def test_range_rids(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(1,1)
    tree.insert_value(1,2)
    tree.insert_value(2,3)
    tree.insert_value(3,4)
    tree.insert_value(0,5)

    # check to see if tree is correct
    self.assertEqual(tree.root.get_keys(), [2])
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [0,1])
    self.assertEqual(tree.root.child_nodes[0].get_rids(), [{5},{1,2}])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [2,3])
    self.assertEqual(tree.root.child_nodes[1].get_rids(), [{3},{4}])

    # # get RID range from same inner node
    # print(tree.get_rids_range_search(0,1))
    # self.assertEqual(tree.get_rids_range_search(0,1), {5,1,2})

    # # get RID range between two inner nodes
    # self.assertEqual(tree.get_rids_range_search(0,2), {5,1,2,3})

  def test_get_all_rids(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 6):
      tree.insert_value(i,i)

    # make sure nodes are proper is_leaf boolean
    self.assertEqual(tree.root.is_leaf, False)
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[i].is_leaf, False)
      for j in range(2):
        self.assertEqual(tree.root.child_nodes[i].child_nodes[j].is_leaf, True)

    # make sure leaf nodes have RIDs
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].get_rids(), [{1}])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_rids(), [{2}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_rids(), [{3}])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_rids(), [{4},{5}])

    # test get range
    self.assertEqual(tree.get_rids_range_search(1,5), {1,2,3,4,5})

  def test_large_dataset(self):
    tree = Column_Index_Tree(4)
    for i in range(1,11):
      tree.insert_value(11 - i,i)

    for i in range(1,11):
      self.assertEqual(tree.get_rids_equality_search(11 - i), {i})
    # print(tree.get_rids_range_search(1,11))
    # self.assertEqual(tree.get_rids_range_search(1,11), {i for i in range(1,11)})

  def test_random_inserts_node_pointers(self):
    tree = Column_Index_Tree(3)
    tree.insert_value(12,1)
    tree.insert_value(11,2)
    tree.insert_value(20,3)
    self.assertEqual(tree.root.child_nodes[0].next_node,
                     tree.root.child_nodes[1])

    tree.insert_value(2,4)
    tree.insert_value(18,5)
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [2,11])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [12])
    self.assertEqual(tree.root.child_nodes[0].next_node,
                     tree.root.child_nodes[1])

    tree.insert_value(5,6)
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_keys(), [5,11])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_keys(), [12])
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[i].is_leaf, False)
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].prev_node, None)
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].next_node,
                     tree.root.child_nodes[0].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].prev_node,
                     tree.root.child_nodes[0].child_nodes[0])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].next_node,
                     tree.root.child_nodes[1].child_nodes[0])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].prev_node,
                     tree.root.child_nodes[0].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].next_node,
                     tree.root.child_nodes[1].child_nodes[1])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].next_node, None)

    tree.insert_value(14,7)
    tree.insert_value(15,8)
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_keys(), [5,11])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[0].get_keys(), [12])
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[i].is_leaf, False)
      self.assertEqual(tree.root.child_nodes[i].next_node, None)
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].next_node,
                     tree.root.child_nodes[1].child_nodes[0])

  def test_random_inserts_node_pointers(self):
    tree = Column_Index_Tree(3)
    seed(9000)

    key_dict = dict()
    for i in range(1,22):
      key = randint(1,22)
      while key in key_dict:
        key = randint(1,22)
      tree.insert_value(key,i)
      key_dict[key] = i


    num_levels = 0
    node = tree.root
    while not node.is_leaf:
      node = node.child_nodes[0]
      num_levels += 1
    self.assertEqual(num_levels, 3)

    # get leaf nodes
    leaf_nodes:list[Column_Index_Node] = []
    for i in range(len(tree.root.child_nodes)):
      for j in range(len(tree.root.child_nodes[i].child_nodes)):
        for k in range(len(tree.root.child_nodes[i].child_nodes[j].child_nodes)):
          self.assertTrue(tree.root.child_nodes[i].child_nodes[j].child_nodes[k].is_leaf)
          leaf_nodes.append(tree.root.child_nodes[i].child_nodes[j].child_nodes[k])

    # check tree
    ## root
    self.assertEqual(tree.root.is_leaf, False)
    self.assertEqual(tree.root.parent, None)
    self.assertEqual(tree.root.next_node, None)
    self.assertEqual(tree.root.get_keys(), [12])
    self.assertEqual(tree.root.get_rids(), [])
    ## 1st inner nodes
    self.assertEqual(len(tree.root.child_nodes), 2)
    for i in range(2):
      self.assertEqual(tree.root.child_nodes[i].is_leaf, False)
      self.assertEqual(tree.root.child_nodes[i].parent, tree.root)
      self.assertEqual(tree.root.child_nodes[i].next_node, None)
      self.assertEqual(tree.root.child_nodes[i].get_rids(), [])
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [6,8])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [15,20])
    ## 2nd inner nodes
    second_inner_nodes:list[Column_Index_Node] = []
    for i in range(2):
      self.assertEqual(len(tree.root.child_nodes[i].child_nodes), 3)
      for j in range(3):
        test_node = tree.root.child_nodes[i].child_nodes[j]
        second_inner_nodes.append(test_node)
        self.assertEqual(test_node.is_leaf, False)
        self.assertEqual(test_node.parent, tree.root.child_nodes[i])
        self.assertEqual(test_node.next_node, None)
        self.assertEqual(test_node.get_rids(), [])
    self.assertEqual([node.get_keys() for node in second_inner_nodes],
                     [[3,5],[7],[10],[14],[16,18],[21]])
    # leaf nodes
    leaf_node_evs = {
      0: [2],
      1: [3,4],
      2: [5],
      3: [6],
      4: [7],
      5: [8,9],
      6: [10,11],
      7: [12,13],
      8: [14],
      9: [15],
      10: [16,17],
      11: [18,19],
      12: [20],
      13: [21,22]
    }
    leaf_nodes:list[Column_Index_Node] = []
    for i in range(2):
      for j in range(3):
        for k in range(len(tree.root.child_nodes[i].child_nodes[j].child_nodes)):
          test_node = tree.root.child_nodes[i].child_nodes[j].child_nodes[k]
          leaf_nodes.append(test_node)
          self.assertEqual(test_node.is_leaf, True)
          self.assertEqual(test_node.parent, tree.root.child_nodes[i].child_nodes[j])
          self.assertEqual(test_node.child_nodes, [])
    for i, ln in enumerate(leaf_nodes):
      self.assertEqual(ln.get_keys(), leaf_node_evs[i])
      if i + 1 == len(leaf_nodes):
        self.assertEqual(ln.next_node, None)
      else:
        self.assertEqual(ln.next_node, leaf_nodes[i+1])

  def test_identified_sum_error(self):
    SEED_VAL = 3562901
    NUM_RECORDS = 1000
    NUM_COLUMNS = 5
    ORDER = 4

    index = Index(NUM_COLUMNS, ORDER)
    records_dict = dict()
    rids_dict = dict()
    key_dict = dict()

    seed(SEED_VAL)
    rid_val = 1
    for _ in range(NUM_RECORDS):
      key = 92106429 + randint(0, NUM_RECORDS)
      while key in records_dict:
        key = 92106429 + randint(0, NUM_RECORDS)
      records_dict[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
      rids_dict[rid_val] = key
      key_dict[key] = rid_val
      for i, entry_val in enumerate(records_dict[key]):
        index.indices[i].insert_value(entry_val, rid_val)
      rid_val += 1

    tree = index.indices[0]
    leaf_nodes:list[Column_Index_Node] = []
    for i in range(len(tree.root.child_nodes)):
      for j in range(len(tree.root.child_nodes[i].child_nodes)):
        for k in range(len(tree.root.child_nodes[i].child_nodes[j].child_nodes)):
          for l in range(len(tree.root.child_nodes[i].child_nodes[j].child_nodes[k].child_nodes)):
            for m in range(len(tree.root.child_nodes[i].child_nodes[j].child_nodes[k].child_nodes[l].child_nodes)):
              for n in range(len(tree.root.child_nodes[i].child_nodes[j].child_nodes[k].child_nodes[l].child_nodes[m].child_nodes)):
                self.assertTrue(tree.root.child_nodes[i].child_nodes[j].child_nodes[k].child_nodes[l].child_nodes[m].child_nodes[n].is_leaf)
                leaf_nodes.append(tree.root.child_nodes[i].child_nodes[j].child_nodes[k].child_nodes[l].child_nodes[m].child_nodes[n])

    for i, leaf_node in enumerate(leaf_nodes):
      if i + 1 == len(leaf_nodes):
        self.assertEqual(leaf_node.next_node, None)
      else:
        self.assertEqual(leaf_node.next_node, leaf_nodes[i+1])

    # 92106481, 92106818
    # 92107133 , 92107162
    LOWER_BOUND = 92107073
    UPPER_BOUND = 92107094

    rids = index.indices[0].get_rids_range_search(LOWER_BOUND, UPPER_BOUND)
    for rid in rids:
      self.assertTrue(LOWER_BOUND <= rids_dict[rid] and rids_dict[rid] <= UPPER_BOUND)

  def test_delete_invalid(self):
    tree = Column_Index_Tree(3)
    seed(100)
    vals_set = set()
    for i in range(1, 11):
      val = randint(1, 99)
      if val in vals_set:
        val = randint(1, 99)
      tree.insert_value(val, i)
      vals_set.add(val)

    temp_io = io.StringIO()
    sys.stdout = temp_io
    tree.delete_entry(101, 123)
    self.assertEqual(temp_io.getvalue(), "Error: Entry value 101 not found.\n")
    temp_io.truncate()
    temp_io.seek(0)
    tree.delete_entry(19, 2)
    self.assertEqual(temp_io.getvalue(), "Error: RID value 2 is not associated with entry value 19.\n")
    sys.stdout = sys.__stdout__

  def test_delete_case_1(self):
    tree = Column_Index_Tree(3)
    seed(100)
    vals_set = set()
    for i in range(1, 11):
      val = randint(1, 99)
      if val in vals_set:
        val = randint(1, 99)
      tree.insert_value(val, i)
      vals_set.add(val)

    # 19 1
    # 59 2
    # 99 3
    # 23 4
    # 91 5
    # 51 6
    # 94 7
    # 45 8
    # 56 9
    # 65 10

    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [51,56])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].is_leaf)
    old_left = tree.root.child_nodes[1].child_nodes[1].prev_node
    old_right = tree.root.child_nodes[1].child_nodes[1].next_node
    tree.delete_entry(56, 9)
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].is_leaf)
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [51])
    new_left = tree.root.child_nodes[1].child_nodes[1].prev_node
    new_right = tree.root.child_nodes[1].child_nodes[1].next_node
    self.assertEqual(old_left, new_left)
    self.assertEqual(old_right, new_right)

    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [59,65])
    self.assertTrue(tree.root.child_nodes[2].child_nodes[0].is_leaf)
    old_left = tree.root.child_nodes[2].child_nodes[0].prev_node
    old_right = tree.root.child_nodes[2].child_nodes[0].next_node
    tree.delete_entry(65, 10)
    self.assertTrue(tree.root.child_nodes[2].child_nodes[0].is_leaf)
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [59])
    new_left = tree.root.child_nodes[2].child_nodes[0].prev_node
    new_right = tree.root.child_nodes[2].child_nodes[0].next_node
    self.assertEqual(old_left, new_left)
    self.assertEqual(old_right, new_right)

    self.assertEqual(tree.root.child_nodes[2].child_nodes[2].get_keys(), [94,99])
    self.assertTrue(tree.root.child_nodes[2].child_nodes[2].is_leaf)
    old_left = tree.root.child_nodes[2].child_nodes[2].prev_node
    old_right = tree.root.child_nodes[2].child_nodes[2].next_node
    tree.delete_entry(99, 3)
    self.assertTrue(tree.root.child_nodes[2].child_nodes[2].is_leaf)
    self.assertEqual(tree.root.child_nodes[2].child_nodes[2].get_keys(), [94])
    new_left = tree.root.child_nodes[2].child_nodes[2].prev_node
    new_right = tree.root.child_nodes[2].child_nodes[2].next_node
    self.assertEqual(old_left, new_left)
    self.assertEqual(old_right, new_right)

  def test_delete_case_2(self):
    tree = Column_Index_Tree(3)
    seed(100)
    vals_set = set()
    for i in range(1, 11):
      val = randint(1, 99)
      if val in vals_set:
        val = randint(1, 99)
      tree.insert_value(val, i)
      vals_set.add(val)

    # 19 1
    # 59 2
    # 99 3
    # 23 4
    # 91 5
    # 51 6
    # 94 7
    # 45 8
    # 56 9
    # 65 10

    self.assertEqual(tree.root.child_nodes[2].get_keys(), [91,94])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[2].get_keys(), [94,99])
    tree.delete_entry(94, 7)
    self.assertEqual(tree.root.child_nodes[2].get_keys(), [91,99])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[2].get_keys(), [99])

    self.assertEqual(tree.root.child_nodes[1].get_keys(), [51])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [51,56])
    tree.delete_entry(51, 6)
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [56])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [56])

    self.assertEqual(tree.root.get_keys(), [45,59])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [59,65])
    tree.delete_entry(59, 2)
    self.assertEqual(tree.root.get_keys(), [45,65])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [65])

  def test_delete_case_3_left_non_leaf_above_leaf(self):
    tree = Column_Index_Tree(3)
    seed(100)
    vals_set = set()
    for i in range(1, 11):
      val = randint(1, 99)
      if val in vals_set:
        val = randint(1, 99)
      tree.insert_value(val, i)
      vals_set.add(val)

    self.assertEqual(tree.root.child_nodes[2].get_keys(), [91,94])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [59,65])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[1].get_keys(), [91])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[2].get_keys(), [94,99])
    tree.delete_entry(91, 5)
    self.assertEqual(tree.root.child_nodes[2].get_keys(), [65,94])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [59])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[1].get_keys(), [65])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[2].get_keys(), [94,99])

  def test_delete_case_3_left_deep_non_leaf(self):
    tree = Column_Index_Tree(3)
    seed(100)
    vals_list = list()
    for i in range(1, 19):
      val = randint(1, 99)
      if val in vals_list:
        val = randint(1, 99)
      tree.insert_value(val, i)
      vals_list.append(val)

    # 19 1
    # 59 2
    # 99 3
    # 23 4
    # 91 5
    # 51 6
    # 94 7
    # 45 8
    # 56 9
    # 65 10
    # 15 11
    # 69 12
    # 16 13
    # 11 14
    # 95 15
    # 34 16
    # 7 17
    # 85 18

    self.assertEqual(tree.root.child_nodes[0].get_keys(), [16,45])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_keys(), [23])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].child_nodes[1].get_keys(), [23,34])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].get_keys(), [51])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[0].get_keys(), [45])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[1].get_keys(), [51,56])
    tree.delete_entry(45, 8)
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [16,34])
    self.assertEqual(tree.root.child_nodes[0].entry_values[1].is_leaf, False)
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_keys(), [23])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].child_nodes[1].get_keys(), [23])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].get_keys(), [51])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[0].get_keys(), [34])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[0].entry_values[0].is_leaf, True)
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[1].get_keys(), [51,56])

  def test_delete_case_3_right_non_leaf_above_leaf(self):
    return
    tree = Column_Index_Tree(3)
    seed(100)
    vals_list = list()
    for i in range(1, 19):
      val = randint(1, 99)
      if val in vals_list:
        val = randint(1, 99)
      tree.insert_value(val, i)
      vals_list.append(val)
    tree.delete_entry(45, 8)

    self.assertEqual(tree.root.child_nodes[0].get_keys(), [16,34])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].get_keys(), [23])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[1].child_nodes[1].get_keys(), [23])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].get_keys(), [51])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[0].get_keys(), [34])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[1].get_keys(), [51,56])
    print("testing here")
    tree.delete_entry(34, 16)
    self.assertEqual(tree.root.child_nodes[0].get_keys(), [16,51])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].get_keys(), [56])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[0].get_keys(), [51])
    self.assertEqual(tree.root.child_nodes[0].child_nodes[2].child_nodes[1].get_keys(), [56])

  def test_delete_case_4(self):
    return
    tree = Column_Index_Tree(3)
    seed(100)
    vals_set = set()
    for i in range(1, 11):
      val = randint(1, 99)
      if val in vals_set:
        val = randint(1, 99)
      tree.insert_value(val, i)
      vals_set.add(val)
    tree.delete_entry(51, 6)

    self.assertEqual(tree.root.get_keys(), [45,59])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [56])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [56])
    self.assertEqual(tree.root.child_nodes[2].get_keys(), [91,94])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [59,65])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[1].get_keys(), [91])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[2].get_keys(), [94,99])
    tree.delete_entry(56, 9)
    self.assertEqual(tree.root.get_keys(), [45,91])
    self.assertEqual(tree.root.child_nodes[1].get_keys(), [59])
    self.assertEqual(tree.root.child_nodes[1].child_nodes[1].get_keys(), [59,65])
    self.assertEqual(tree.root.child_nodes[2].get_keys(), [94])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[0].get_keys(), [91])
    self.assertEqual(tree.root.child_nodes[2].child_nodes[1].get_keys(), [94,99])


if __name__ == "__main__":
  main()
