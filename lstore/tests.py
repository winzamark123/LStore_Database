from unittest import TestCase, main
from index import Column_Index_Tree

# NOTE: you have to start your functions w/ 'test_'
# link about it: https://www.freecodecamp.org/news/how-to-write-unit-tests-for-python-functions/#:~:text=Each%20of%20the%20tests%20that,methods%20should%20start%20with%20test_%20.

class Test_Index(TestCase):
  def test_non_full_root(self):
    tree = Column_Index_Tree(5)
    self.assertTrue(len(tree) == 0)
    tree.insert_value("apple", 1)
    tree.insert_value("hello", 2)
    tree.insert_value("hi", 3)
    tree.insert_value("zucchini", 4)
    self.assertTrue(len(tree) == 4)
    self.assertTrue(tree.root.is_full() == False)
    self.assertTrue(len(tree.root.child_nodes) == 0)

  def test_full_root_split(self):
    tree = Column_Index_Tree(3)
    tree.insert_value("hey", 1)
    tree.insert_value("whats up", 2)
    tree.insert_value("hello", 3)
    self.assertTrue(tree.root.entry_values == ["hey"])
    self.assertTrue(tree.root.child_nodes[0].entry_values == ["hello"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hey", "whats up"])

    # test that each child node's parent is root
    self.assertTrue(tree.root.child_nodes[0].parent == tree.root)
    self.assertTrue(tree.root.child_nodes[1].parent == tree.root)

  def test_after_full_root(self):
    tree = Column_Index_Tree(4)
    tree.insert_value("apple", 1)
    tree.insert_value("hello", 2)
    tree.insert_value("hi", 3)

    # split root
    tree.insert_value("zucchini", 4)
    self.assertTrue(tree.root.entry_values[0] == "hi")
    self.assertTrue(tree.root.child_nodes[0].entry_values == ["apple", "hello"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hi", "zucchini"])
    tree.insert_value("poppy", 5)
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hi", "poppy", "zucchini"])

    tree.insert_value("teacup", 6)
    self.assertTrue(tree.root.entry_values == ["hi", "teacup"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hi", "poppy"])
    self.assertTrue(tree.root.child_nodes[2].entry_values == ["teacup", "zucchini"])

    # test if parent of child nodes is root
    self.assertTrue(tree.root.child_nodes[0].parent == tree.root)
    self.assertTrue(tree.root.child_nodes[1].parent == tree.root)
    self.assertTrue(tree.root.child_nodes[2].parent == tree.root)

  def test_one_inner_level(self):
    # initiate index tree
    ## apple, hola
    tree = Column_Index_Tree(3)
    tree.insert_value("apple", 1)
    tree.insert_value("hola", 2)

    # split root
    ## apple, armful, hola, pie
    tree.insert_value("pie", 3)
    self.assertTrue(tree.root.entry_values == ["hola"])
    tree.insert_value("armful", 4)
    self.assertTrue(tree.root.child_nodes[0].entry_values == ["apple", "armful"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hola", "pie"])

    # split 2nd child node (index 0 of root child node) + fill up all child nodes
    ## apple, armful, hola, llama, pie, triangle
    tree.insert_value("triangle", 5)
    self.assertTrue(tree.root.entry_values == ["hola", "pie"])
    self.assertTrue(tree.root.child_nodes[0].entry_values == ["apple", "armful"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hola"])
    self.assertTrue(tree.root.child_nodes[2].entry_values == ["pie", "triangle"])
    tree.insert_value("llama", 6)
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hola", "llama"])

    # split a child node which splits root node
    ## apple, armful, hola, llama, pie, triangle, trie
    tree.insert_value("trie", 7)
    self.assertTrue(tree.root.entry_values == ["pie"])
    self.assertTrue(tree.root.child_nodes[0].entry_values == ["hola"])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[0].entry_values == ["apple", "armful"])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[1].entry_values == ["hola", "llama"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["triangle"])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].entry_values == ["pie"])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].entry_values == ["triangle", "trie"])

  def test_split_to_two_inner_levels(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 19):
      tree.insert_value(i,i)

    # TODO

  def test_check_node_pointers_two_nodes(self):
    tree = Column_Index_Tree(3)
    for i in range(1,4):
      tree.insert_value(i,i)
    
    self.assertEqual(tree.root.child_nodes[0].next_node,
                     tree.root.child_nodes[1])

  def test_check_next_node_pointers(self):
    tree = Column_Index_Tree(3)
    for i in range(1,6):
      tree.insert_value(i,i)
    
    # make sure that tree is correct
    self.assertTrue(tree.root.entry_values == [3])
    self.assertTrue(tree.root.child_nodes[0].entry_values == [2])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[0].entry_values == [1])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[1].entry_values == [2])
    self.assertTrue(tree.root.child_nodes[1].entry_values == [4])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].entry_values == [3])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].entry_values == [4, 5])
    for i in range(2):
      for j in range(2):
        self.assertTrue(tree.root.child_nodes[i].child_nodes[j].is_leaf)
    
    # check next node pointers in leaf nodes
    # TODO: fix merging + node splitting algorithms to deal w/ node pointers
    self.assertEqual(tree.root.child_nodes[0].child_nodes[0].next_node,
                     tree.root.child_nodes[0].child_nodes[1])


  def test_get_rids_of_entry(self):
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

    # get RID range within same node
    self.assertTrue(tree.get_rids_range_search(0,1) == [5,1,2])

    # get RID range within two nodes
    self.assertTrue(tree.get_rids_range_search(0,2) == [5,1,2,3])

  def test_get_all_rids(self):
    tree = Column_Index_Tree(3)
    for i in range(1, 6):
      tree.insert_value(i,i)

    # make sure leaf nodes have RIDs
    self.assertTrue(tree.root.child_nodes[0].child_nodes[0].rids == [[1]])
    self.assertTrue(tree.root.child_nodes[0].child_nodes[1].rids == [[2]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[0].rids == [[3]])
    self.assertTrue(tree.root.child_nodes[1].child_nodes[1].rids == [[4],[5]])

    # get RID range within two leaf nodes deriving from different inner nodes
    # self.assertTrue(tree.get_rids_range_search(2,3))

    # self.assertTrue(tree.get_rids_range_search(1,5) == [1,2,3,4,5])


if __name__ == "__main__":
  main()

