#!/usr/bin/env python3

import unittest


class Node(object):
	__slots__ = ["key", "value", "children"]
	def __init__(self, key):
		self.key = key
		self.value = None
		self.children = dict()

	def _insert_rec(self, key, value):
		if len(key) == 0:
			self.value = value
			return
		c = key[0]
		if c not in self.children:
			self.children[c] = Node(c)
		self.children[c]._insert_rec(key[1:], value)

	def _insert_it(self, key, value):
		node = self
		for c in key:
			if c not in node.children:
				node.children[c] = Node(c)
			node = node.children[c]
		node.value = value

	def _find_rec(self, key):
		if len(key) == 0:
			return self.value
		c = key[0]
		if c not in self.children:
			return None
		return self.children[c]._find_rec(key[1:])

	def _find_it(self, key):
		node = self
		for c in key:
			if c not in node.children:
				return None
			node = node.children[c]
		return node.value

	def insert(self, key, value):
		self._insert_it(key, value)
		# self._insert_rec(key, value)

	def find(self, key):
		return self._find_it(key)
		# return self._find_rec(key)

	def dfs(self, current_path=[]):
		for k, v in self.children.items():
			current_path.append(k)
			yield from v.dfs(current_path)
			del current_path[-1]
		yield (current_path, self)

	def __repr__(self):
		return "Node(key={},value={},children={})".format(self.key, self.value, self.children.keys())

	def __str__(self):
		return str("({},{})".format(self.key, self.value))


class Trie(object):
	def __init__(self):
		self.root = Node(None)

	def insert(self, key, value):
		self.root.insert(key, value)

	def find(self, key):
		return self.root.find(key)

	def dfs(self):
		return self.root.dfs()

	def get_values(self):
		current_path = []
		for (path, node) in self.root.dfs():
			if node.value is not None:
				yield (path, node.value)

	def __repr__(self):
		return repr(self.root)


class TestTrie(unittest.TestCase):

	def test_1(self):
		trie = Trie()
		key, value = "abc123", "success"
		trie.insert(key, value)
		self.assertEqual(trie.find(key), value)

	def test_2(self):
		trie = Trie()
		key, value = "abc345", "yay"
		trie.insert(key, value)
		self.assertEqual(trie.find(key), value)

	def test_3(self):
		trie = Trie()
		key, value = "abc123", "success"
		trie.insert(key, value)
		trie.insert("abc345", "yay")
		self.assertEqual(trie.find(key), value)

	def test_4(self):
		trie = Trie()
		self.assertIsNone(trie.find("shouldNOTexist"))

	def test_5(self):
		values = set([("abc123", "success"), ("abc345", "yay")])
		trie = Trie()
		for key, value in values:
			trie.insert(key, value)
		res = set(map(lambda x: ("".join(x[0]), x[1]), trie.get_values()))
		# print(values.difference(res), res.difference(values))
		self.assertTrue(len(values.difference(res)) == 0 and len(res.difference(values)) == 0)


def run_tests():
	unittest.main()


def print_test():
	trie = Trie()
	trie.insert("abc123", "success")
	trie.insert("abc345", "yay")
	trie.insert("abc3478", "zuzu")
	print("[dfs]")
	print("\n".join(map(lambda x: "(path={},node={})".format(x[0], str(x[1])),trie.dfs())))
	print("[get_values]")
	print("\n".join(map(lambda x: "(path={},value={})".format("".join(x[0]), x[1]), trie.get_values())))



if __name__ == "__main__":
	print_test()
	run_tests()
