import os
import sys


class DataManager(object):
	MODE_READ, MODE_WRITE = "read", "write"

	def __init__(self):
		self.mode = None

	def set_mode_read(self):
		self.mode = self.MODE_READ

	def set_mode_write(self):
		self.mode = self.MODE_WRITE

	def close(self):
		raise Exception("Not implemented")

	def reset(self):
		raise Exception("Not implemented")

	def readTuple(self):
		raise Exception("Not implemented")

	def writeTuple(self):
		raise Exception("Not implemented")


class MemDataManager(object):
	def __init__(self):
		DataManager.__init__(self)

	def close(self):
		pass

	def reset(self):
		# TODO
		pass

	def readTuple(self):
		# TODO
		pass

	def writeTuple(self):
		# TODO
		pass


class StdinDataManager(object):
	def __init__(self, fdelim):
		DataManager.__init__(self)
		self.fdelim = fdelim
		self.fd = os.fdopen(os.dup(sys.stdin.fileno()))

	def close(self):
		fd.close()

	def reset(self):
		# TODO
		pass

	def readTuple(self):
		# TODO
		pass

	def writeTuple(self):
		raise Exception("Operation not supported")


class FileDataManager(object):
	def __init__(self, fdelim, in_file):
		DataManager.__init__(self)
		self.fdelim = fdelim
		self.fd = open(in_file, 'r')

	def close(self):
		fd.close()

	def reset(self):
		# TODO
		pass

	def readTuple(self):
		# TODO
		pass

	def writeTuple(self):
		raise Exception("Operation not supported")
