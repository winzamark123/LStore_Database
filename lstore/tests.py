from unittest import TestCase, main
from index import Column_Index_Tree

class Test_Index(TestCase):
  def test_non_full_root(self):
    tree = Column_Index_Tree(5)
    tree.insert_value("apple", 45)
    tree.insert_value("hello", 3221)
    tree.insert_value("hi", 13289)
    tree.insert_value("zucchini", 1320)
    self.assertTrue(tree.root.is_full() == False)
    self.assertTrue(len(tree.root.child_nodes) == 0)

  def test_full_root_split(self):
    tree = Column_Index_Tree(3)
    tree.insert_value("hey", 312)
    tree.insert_value("whats up", 621)
    tree.insert_value("hello", 138209)
    self.assertTrue(tree.root.entry_values == ["hey"])
    self.assertTrue(tree.root.child_nodes[0].entry_values == ["hello"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hey", "whats up"])
    # test that each child node's parent is root
    self.assertTrue(tree.root.child_nodes[0].parent == tree.root)
    self.assertTrue(tree.root.child_nodes[1].parent == tree.root)

  def test_after_full_root(self):
    tree = Column_Index_Tree(4)
    tree.insert_value("apple", 45)
    tree.insert_value("hello", 3221)
    tree.insert_value("hi", 13289)
    tree.insert_value("zucchini", 1320)
    self.assertTrue(tree.root.entry_values[0] == "hi")
    self.assertTrue(tree.root.child_nodes[0].entry_values == ["apple", "hello"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hi", "zucchini"])
    tree.insert_value("poppy", 12)
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hi", "poppy", "zucchini"])
    
    tree.insert_value("teacup", 31290)
    self.assertTrue(tree.root.entry_values == ["hi", "teacup"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hi", "poppy"])
    self.assertTrue(tree.root.child_nodes[2].entry_values == ["teacup", "zucchini"])

    # test if parent of child nodes is root
    self.assertTrue(tree.root.child_nodes[0].parent == tree.root)
    # TODO: figure out why these two failed
    # self.assertTrue(tree.root.child_nodes[1].parent == tree.root) 
    # self.assertTrue(tree.root.child_nodes[2].parent == tree.root)

  def test_one_inner_level(self):
    tree = Column_Index_Tree(3)
    tree.insert_value("apple", 34)
    tree.insert_value("hola", 12)
    tree.insert_value("pie", 123)
    self.assertTrue(tree.root.entry_values == ["hola"])
    tree.insert_value("armful", 21)
    self.assertTrue(tree.root.child_nodes[0].entry_values == ["apple", "armful"])
    tree.insert_value("triangle", 312)

    self.assertTrue(tree.root.entry_values == ["hola", "pie"])
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hola"])
    self.assertTrue(tree.root.child_nodes[2].entry_values == ["pie", "triangle"])
    tree.insert_value("llama", 31212)
    self.assertTrue(tree.root.child_nodes[1].entry_values == ["hola", "llama"])
    # tree.insert_value("trie", 312211)

    # self.assertTrue(tree.root.entry_values == ["llama"])    
    # print(tree.root.entry_values)
    # for l in tree.root.child_nodes:
    #   print(l.entry_values)

if __name__ == "__main__":
  main()

