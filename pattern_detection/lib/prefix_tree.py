#!/usr/bin/env python3

import unittest


class Node(object):
	__slots__ = ["key", "count", "subtree_count", "children"]
	def __init__(self, key):
		self.key = key
		self.count = 0
		self.subtree_count = 0
		self.children = dict()

	def insert(self, key):
		node = self
		for c in key:
			if c not in node.children:
				node.children[c] = Node(c)
			node.subtree_count += 1
			node = node.children[c]
		node.count += 1

	def find(self, key):
		node = self
		for c in key:
			if c not in node.children:
				return None
			node = node.children[c]
		return node

	def dfs(self, current_path=[]):
		for k, v in self.children.items():
			current_path.append(k)
			yield from v.dfs(current_path)
			del current_path[-1]
		yield (current_path, self)

	def __repr__(self):
		return "Node(key={},count={},subtree_count={},children={})".format(self.key, self.count, self.subtree_count, self.children.keys())

	def __str__(self):
		return str("(key={},count={},subtree_count={})".format(self.key, self.count, self.subtree_count))


class PrefixTree(object):
	def __init__(self):
		self.root = Node(None)

	def insert(self, key):
		self.root.insert(key)

	def find(self, key):
		return self.root.find(key)

	def dfs(self):
		return self.root.dfs()

	def get_items(self):
		current_path = []
		for (path, node) in self.root.dfs():
			if node.count > 0:
				yield (path, node.count)

	def __repr__(self):
		return repr(self.root)


class TestPrefixTree(unittest.TestCase):

	def test_1(self):
		trie = PrefixTree()
		key = "abc123"
		trie.insert(key)
		self.assertEqual(trie.find(key).count, 1)

	def test_2(self):
		trie = PrefixTree()
		key = "abc123"
		trie.insert(key)
		trie.insert(key)
		self.assertEqual(trie.find(key).count, 2)

	def test_3(self):
		trie = PrefixTree()
		self.assertIsNone(trie.find("shouldNOTexist"))

	def test_4(self):
		trie = PrefixTree()
		key = "abc123"
		trie.insert(key)
		trie.insert("abc123xyz")
		self.assertEqual(trie.find(key).count, 1)

	def test_5(self):
		trie = PrefixTree()
		trie.insert("abc123")
		trie.insert("abc123xyz")
		self.assertEqual(trie.find("abc12").subtree_count, 2)

	def test_6(self):
		trie = PrefixTree()
		trie.insert("abc123")
		trie.insert("abc123xyz")
		self.assertEqual(trie.find("abc123x").subtree_count, 1)

	def test_7(self):
		trie = PrefixTree()
		trie.insert("abc123")
		trie.insert("abc123xyz")
		self.assertEqual(trie.find("abc123xyz").subtree_count, 0)


def run_tests():
	unittest.main()


def print_test():
	trie = PrefixTree()
	trie.insert("abc123")
	trie.insert("abc345")
	trie.insert("abc3478")
	trie.insert("abc345")
	print("[dfs]")
	print("\n".join(map(lambda x: "(path={},node={})".format(x[0], str(x[1])),trie.dfs())))
	print("[get_items]")
	print("\n".join(map(lambda x: "(path={},count={})".format("".join(x[0]), x[1]), trie.get_items())))



if __name__ == "__main__":
	print_test()
	run_tests()
