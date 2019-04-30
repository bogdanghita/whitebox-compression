import os
import sys
from copy import deepcopy
from statistics import mean, median
from lib.util import *
from lib.prefix_tree import PrefixTree
from lib.datatype_analyzer import NumericDatatypeAnalyzer


class OperatorException(Exception):
	pass


class PatternDetector(object):
	def __init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage):
		self.pattern_log = pattern_log
		self.expr_tree = expr_tree
		self.null_value = null_value
		self.min_col_coverage = min_col_coverage
		self.row_count = 0
		self.name = self.__class__.__name__
		self.columns = {}

	def init_columns(self, columns):
		for idx, col in enumerate(columns):
			if self.select_column(col):
				self.columns[idx] = self.empty_col_item(col)

	def select_column(self, col):
		return True

	@classmethod
	def empty_col_item(cls, col):
		return {
			"info": deepcopy(col),
			"patterns": {
				"default": {"rows": [], "details": {}},
				# NOTE: pattern detectors with only one pattern should use the "default" pattern;
				# multi-pattern detectors should add a new entry for each pattern;
				# "default" can be used if there is a main pattern or it can be left empty
			}
		}

	def get_signature(self):
		return self.name

	@classmethod
	def is_null(cls, attr, null_value):
		return attr == null_value

	def feed_tuple(self, tpl):
		self.row_count += 1

	def evaluate(self):
		'''Evaluates the pattern based on the data fed so far

		Returns:
			columns: dict(col_id, patterns) columns analyzed by the pattern detector, where:
				col_id: str # id of the column
				patterns: list(
					dict(
						p_id: # id of the pattern
						score: float # number between 0 and 1 indicating how well the column fits this pattern
						rows: [row_id: int, ...], # rows where the pattern applies; indexed from 0
						res_columns: [rcol_1, rcol_2, ...], # list of resulting columns; type: util.Column
						operator_info: dict(), # operator parameters (used when applying the transformation)
						details: dict(), # pattern-specific info
						pattern_signature: str # unique signature of the pattern (for comparison purposes)
					)
				)
		'''
		return dict()

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		'''
		Returns: a function with the following signature:
				 params: attrs
				 returns: attrs_out
				 raises: Exception if attrs are invalid (i.e. pattern and/or params are not applicable)
				 properties:
				 	len(attrs) = len(cols_in)
					len(attrs_out) = len(cols_out)
				 note-1: function assumes that the above properties are satisfied & does not check them
				 note-2: get_operator() function is meant to be called only in the initialization phase, not for every tuple; e.g. call get_operator once for each expression node and save the returned operator, then use it as many times as you want
		'''
		raise Exception("Not implemented")


class NullPatternDetector(PatternDetector):
	def __init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage):
		PatternDetector.__init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage)
		self.init_columns(columns)

	def select_column(self, col):
		return col.datatype.nullable

	@classmethod
	def empty_col_item(cls, col):
		return PatternDetector.empty_col_item(col)

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		if self.is_null(attr, self.null_value):
			self.columns[idx]["patterns"]["default"]["rows"].append(self.row_count-1)
		return True

	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)

	def evaluate(self):
		res = dict()
		for idx, col in self.columns.items():
			if len(col["patterns"]["default"]["rows"]) == 0:
				continue
			score = 0 if self.row_count == 0 else len(col["patterns"]["default"]["rows"]) / self.row_count
			p_item = {
				"p_id": "{}:default".format(self.name),
				"score": score,
				"rows": col["patterns"]["default"]["rows"],
				"res_columns": [], # TODO
				"operator_info": dict(), # TODO
				"details": dict(),
				"pattern_signature": self.get_signature()
			}
			patterns = [p_item]
			res[col["info"].col_id] = patterns
		return res


class ConstantPatternDetector(PatternDetector):
	def __init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage):
		PatternDetector.__init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage)
		self.init_columns(columns)

	def select_column(self, col):
		# do not try this pattern again
		p_log = self.pattern_log.get_log(col.col_id)
		if self.get_signature() in p_log:
			return False
		return True

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["nulls"] = []
		return res

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		if self.is_null(attr, self.null_value):
			self.columns[idx]["nulls"].append(self.row_count-1)
			return True
		# TODO
		return True

	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)

	def evaluate(self):
		return dict()
		# TODO


class StringPatternDetector(PatternDetector):
	def __init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage):
		PatternDetector.__init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage)

	def select_column(self, col):
		return col.datatype.name == "varchar"

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["nulls"] = []
		return res

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		if self.is_null(attr, self.null_value):
			self.columns[idx]["nulls"].append(self.row_count-1)
			return True
		return False

	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)


class NumberAsString(StringPatternDetector):
	def __init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage):
		StringPatternDetector.__init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage)
		self.init_columns(columns)

	def select_column(self, col):
		if not StringPatternDetector.select_column(self, col):
			return False
		# do not try this pattern again
		p_log = self.pattern_log.get_log(col.col_id)
		pattern_signature = self.get_signature()
		if pattern_signature in p_log:
			return False
		# do not try this pattern if col is and output column of this exact same pattern
		et_col = self.expr_tree.get_column(col.col_id)
		if et_col is None:
			raise Exception("Column not present in the expression tree: col={}".format(col))
		parent_expr_n = et_col["output_of"]
		if parent_expr_n is not None and pattern_signature == parent_expr_n.pattern_signature:
			return False
		return True
		# TODO: exception columns are not present in the expression tree; => the above check does not work for them
		# TODO: solve this ^; see note as photo on phone

	@classmethod
	def empty_col_item(cls, col):
		res = StringPatternDetector.empty_col_item(col)
		res["ndt_analyzer"] = NumericDatatypeAnalyzer()
		return res

	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		if not NumericDatatypeAnalyzer.is_supported_number(attr):
			return True
		try:
			self.columns[idx]["ndt_analyzer"].feed_attr(attr)
		except Exception as e:
			return True
		self.columns[idx]["patterns"]["default"]["rows"].append(self.row_count-1)
		return True

	def compute_score(self, col):
		null_cnt = len(col["nulls"])
		valid_cnt = len(col["patterns"]["default"]["rows"])
		total_cnt = self.row_count
		if total_cnt == 0:
			return 0
		if float(valid_cnt) / total_cnt < self.min_col_coverage:
			return 0
		return float(valid_cnt) / (total_cnt - null_cnt)

	def build_pattern_data(self, col):
		score = self.compute_score(col)
		# new column info
		ncol_col_id = str(col["info"].col_id) + "_0"
		ncol_name = str(col["info"].name) + "_0"
		ncol_datatype = col["ndt_analyzer"].get_datatype()
		ncol_datatype.nullable = True
		ncol = Column(ncol_col_id, ncol_name, ncol_datatype)
		# operator info
		operator_info = dict(name="identity")
		# pattern data
		p_item = {
			"p_id": "{}:default".format(self.name),
			"score": score,
			"rows": col["patterns"]["default"]["rows"],
			"res_columns": [ncol],
			"operator_info": operator_info,
			"details": dict(),
			"pattern_signature": self.get_signature()
		}
		return p_item

	def evaluate(self):
		res = dict()
		for idx, col in self.columns.items():
			if len(col["patterns"]["default"]["rows"]) == 0:
				continue
			p_item = self.build_pattern_data(col)
			patterns = [p_item]
			res[col["info"].col_id] = patterns
		return res

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		'''
		NOTE: for now we use the identity operator; in the future we may want
			  to actually perform a cast to a numeric type, based on the column
			  datatype
		'''
		def operator(attrs):
			val, c_out = attrs[0], cols_out[0]
			if cls.is_null(val, null_value):
				raise OperatorException("[{}] null value is not supported".format(cls.__name__))
			# NOTE: this filters strings that look like numbers in scientific notation
			if not NumericDatatypeAnalyzer.is_supported_number(val):
				raise OperatorException("[{}] value is not numeric".format(cls.__name__))
			# check if value matches the the datatype of the output column; raise exception if not
			try:
				n_val = NumericDatatypeAnalyzer.cast(val, c_out.datatype)
			except Exception as e:
				raise OperatorException("[{}] value does not match datatype: value={}, datatype={}, err={}".format(cls.__name__, val, c_out.datatype, e))
			# NOTE: [?] in the future maybe return n_val instead of val
			attrs_out = [val]
			return attrs_out

		return operator


class StringCommonPrefix(StringPatternDetector):
	def __init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage):
		StringPatternDetector.__init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage)
		self.prefix_tree = PrefixTree()
		self.init_columns(columns)

	def select_column(self, col):
		if not StringPatternDetector.select_column(self, col):
			return False
		# do not try this pattern again
		p_log = self.pattern_log.get_log(col.col_id)
		if self.get_signature() in p_log:
			return False
		return True

	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		self.prefix_tree.insert(attr)
		return True

	def evaluate(self):
		return dict()
		# TODO


class CharSetSplit(StringPatternDetector):
	def __init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage, default_placeholder, char_sets, empty_string_pattern="<empty_string>", drop_single_char_pattern=True):
		StringPatternDetector.__init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage)
		self.default_placeholder = default_placeholder
		self.char_sets = {c["placeholder"]:c for c in char_sets}
		self.empty_string_pattern = empty_string_pattern
		self.drop_single_char_pattern = drop_single_char_pattern
		self.init_columns(columns)

	def select_column(self, col):
		if not StringPatternDetector.select_column(self, col):
			return False
		# do not try this pattern again
		p_log = self.pattern_log.get_log(col.col_id)
		if self.get_signature() in p_log:
			return False
		return True

	def get_signature(self):
		char_sets_signature = ""
		for char_set in self.char_sets.values():
			c_set = char_set["char_set"]
			char_sets_signature += "[" + ",".join(sorted(list(c_set))) + "]"
		params_signature = "char_sets=[{}]".format(char_sets_signature)
		res = "{}:{}".format(self.name, params_signature)
		# print(res)
		return res

	def get_pattern_string(self, attr):
		pattern_string = []

		for c in attr:
			# look for c in each char set
			for ph, c_set in self.char_sets.items():
				if c not in c_set["char_set"]:
					continue
				break
			# use the default placeholder if not match
			else:
				ph = self.default_placeholder
			# update pattern string (if necessary)
			if len(pattern_string) == 0 or pattern_string[-1] != ph:
				pattern_string.append(ph)

		if len(pattern_string) == 0:
			return self.empty_string_pattern
		else:
			return "".join(pattern_string)

	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		col = self.columns[idx]

		ps = self.get_pattern_string(attr)
		if ps not in col["patterns"]:
			col["patterns"][ps] = {"rows": [], "details": {}}
		col["patterns"][ps]["rows"].append(self.row_count-1)
		return True

	def compute_score(self, col, pattern_s, pattern_s_data):
		null_cnt = len(col["nulls"])
		valid_cnt = len(pattern_s_data["rows"])
		total_cnt = self.row_count
		if total_cnt == 0:
			return 0
		if float(valid_cnt) / total_cnt < self.min_col_coverage:
			return 0
		return float(valid_cnt) / (total_cnt - null_cnt)

	def build_pattern_data(self, col, pattern_s, pattern_s_data):
		res_columns = []
		score = self.compute_score(col, pattern_s, pattern_s_data)
		operator_info = dict(char_sets={}, pattern_string=pattern_s)
		for ph, cs in self.char_sets.items():
			new_cs = deepcopy(cs)
			new_cs["char_set"] = list(new_cs["char_set"])
			operator_info["char_sets"][ph] = new_cs
		operator_info["char_sets"][self.default_placeholder] = {"name": "default", "placeholder": self.default_placeholder, "char_set": []}
		# new columns info
		for idx, ph in enumerate(pattern_s):
			ncol_col_id = str(col["info"].col_id) + "_" + str(idx)
			ncol_name = str(col["info"].name) + "_" + str(idx)
			# NOTE: here we keep the original column datatype (i.e. varchar(x))
			# TODO: think of the possibility of giving a better datatype (e.g. varchar of shorter length, numeric datatype, etc.)
			ncol_datatype = deepcopy(col["info"].datatype)
			ncol_datatype.nullable = True
			ncol = Column(ncol_col_id, ncol_name, ncol_datatype)
			res_columns.append(ncol)
		p_item = {
			"p_id": "{}:{}".format(self.name, pattern_s),
			"score": score,
			"rows": pattern_s_data["rows"],
			"res_columns": res_columns,
			"operator_info": operator_info,
			"details": dict(),
			"pattern_signature": self.get_signature()
		}
		return p_item

	def evaluate(self):
		res = dict()

		for idx, col in self.columns.items():
			patterns = []
			for ps, ps_data in col["patterns"].items():
				if len(ps_data["rows"]) == 0:
					continue
				if self.drop_single_char_pattern and (ps == self.empty_string_pattern or len(ps) == 1):
					continue
				p_item = self.build_pattern_data(col, ps, ps_data)
				patterns.append(p_item)
			res[col["info"].col_id] = patterns

		return res

	@classmethod
	def split_attr(cls, attr, pattern_string, char_sets, default_placeholder, default_inv_charset):
		default_exception = OperatorException("[{}] attr does not match pattern_string: attr={}, pattern_string={}".format(cls.__name__, attr, pattern_string))
		# NOTE: for now, we don't support empty value
		# NOTE: we assume len(pattern_string) > 0
		''' NOTE: we assume that the (pattern_string, char_sets) pair is
		valid, thus we don't check if ph is in char_sets '''

		# print("[split_attr] {}, {}, {}".format(attr, pattern_string, char_sets))

		if len(attr) == 0:
			raise default_exception

		attrs_out = []

		attr_idx = 0
		for ph in pattern_string:
			cnt = 0
			if ph == default_placeholder:
				while attr_idx < len(attr) and attr[attr_idx] not in default_inv_charset:
					cnt += 1
					attr_idx += 1
			else:
				c_set = char_sets[ph]
				while attr_idx < len(attr) and attr[attr_idx] in c_set["char_set"]:
					cnt += 1
					attr_idx += 1
			if cnt == 0:
				raise default_exception
			attrs_out.append(attr[attr_idx-cnt:attr_idx])
		if attr_idx < len(attr):
			raise default_exception

		return attrs_out

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		''' NOTE: c_set["char_set"] got serialized as a list; search is slow in list;
		it is converted back to a set here'''
		default_placeholder = None
		char_sets = {}
		for ph, c_set in operator_info["char_sets"].items():
			char_sets[c_set["placeholder"]] = deepcopy(c_set)
			char_sets[c_set["placeholder"]]["char_set"] = set(char_sets[c_set["placeholder"]]["char_set"])
			if c_set["name"] == "default":
				default_placeholder = c_set["placeholder"]
		default_inv_charset = {c for c_set in char_sets.values() for c in c_set["char_set"]}

		def operator(attrs):
			val = attrs[0]
			if cls.is_null(val, null_value):
				raise OperatorException("[{}] null value is not supported".format(cls.__name__))
			attrs_out = cls.split_attr(val, operator_info["pattern_string"], char_sets, default_placeholder, default_inv_charset)
			return attrs_out

		return operator


class NGramFreqSplit(StringPatternDetector):
	def __init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage, n, case_sensitive=False):
		StringPatternDetector.__init__(self, columns, pattern_log, expr_tree, null_value, min_col_coverage)
		self.n = n
		self.case_sensitive = case_sensitive

	def select_column(self, col):
		return StringPatternDetector.select_column(self, col)

	@classmethod
	def empty_col_item(cls, col):
		res = StringPatternDetector.empty_col_item(col)
		res["ngrams"] = {
			"freqs": {},
			"avg_freq": 0,
			"median_freq": 0
		}
		res["attrs"] = []
		return res

	def get_ngrams(self, attr):
		if not self.case_sensitive:
			attr = attr.lower()

		for i in range(len(attr) - self.n + 1):
			ng = attr[i:i + self.n]
			yield ng

	def get_ngram_freq_mask(self, attr, ngrams, delim="-"):
		if not self.case_sensitive:
			attr = attr.lower()
		ngfm = []

		for i in range(len(attr) - self.n + 1):
			ng = attr[i:i + self.n]
			ng_freq = 0 if ng not in ngrams else ngrams[ng]
			ngfm.append(str(ng_freq))

		return delim.join(ngfm)

	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		col = self.columns[idx]

		# TODO: debug
		# if col["info"].col_id != 28:
		# 	return True

		# update ngram frequencies
		ngrams = self.get_ngrams(attr)
		for ng in ngrams:
			if ng not in col["ngrams"]["freqs"]:
				col["ngrams"]["freqs"][ng] = 0
			col["ngrams"]["freqs"][ng] += 1

		# store the attributes for the evaluate() step
		# TODO: store these values globally, i.e. only once, to avoid storing them twice if more pattern detectors need to do this
		col["attrs"].append(attr)

		return True

	def evaluate(self):
		res = dict()

		# for col in self.columns.values():
		# 	if len(col["ngrams"]["freqs"].keys()) == 0:
		# 		# TODO: do something else here? check with the evaluate() output format
		# 		continue
		# 	print(col["info"], col["attrs"][:10])
		# 	col["ngrams"]["avg_freq"] = mean(col["ngrams"]["freqs"].values())
		# 	col["ngrams"]["median_freq"] = median(col["ngrams"]["freqs"].values())
		# 	print(col["ngrams"]["avg_freq"], col["ngrams"]["median_freq"])
		# 	print(sorted(col["ngrams"]["freqs"].values()))
		# 	for attr in col["attrs"]:
		# 		ngfm = self.get_ngram_freq_mask(attr, col["ngrams"]["freqs"])
		# 		print(attr, ngfm, col["ngrams"]["avg_freq"], col["ngrams"]["median_freq"])

		return res

	def get_ngram_freq_masks(self, delim=","):
		res = {}
		for col in self.columns.values():
			if len(col["ngrams"]["freqs"].keys()) == 0:
				continue
			# NOTE-1: we want to return a generator, thus the usage of round brackets
			# See: https://code-maven.com/list-comprehension-vs-generator-expression
			# NOTE-2: we also want to create a closure for the col variable
			res[col["info"].col_id] = (lambda col: (self.get_ngram_freq_mask(attr, col["ngrams"]["freqs"], delim=delim) for attr in col["attrs"]))(col)
		return res

'''
================================================================================
'''
pattern_detectors = {
	"NullPatternDetector".lower(): NullPatternDetector,
	"ConstantPatternDetector".lower(): ConstantPatternDetector,
	"NumberAsString".lower(): NumberAsString,
	"CharSetSplit".lower(): CharSetSplit,
	"NGramFreqSplit".lower(): NGramFreqSplit,
	"StringCommonPrefix".lower(): StringCommonPrefix,
}
def get_pattern_detector(pattern_id):
	default_exception = Exception("Invalid pattern id")
	try:
		pn = pattern_id.split(":")[0]
	except Exception as e:
		raise default_exception

	pnl = pn.lower()
	if pnl in pattern_detectors:
		return pattern_detectors[pnl]

	raise default_exception
