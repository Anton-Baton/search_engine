
class Document(object):
	def __init__(self, parsed_text, score):
		self.parsed_text = parsed_text
		self.score = score

	def __getitem__(self, i):
		return self.parsed_text[i]

	def __iter__(self):
		return self.parsed_text.__iter__()

	def __len__(self):
		return len(self.parsed_text)


class InvertedIndexHit(object):
	def __init__(self, doc_id, position, score):
		self.doc_id = doc_id
		self.position = position
		self.score = score

	# TODO: find better way to get unique Hits in document
	def __eq__(self, other):
		return self.doc_id == other.doc_id

	def __hash__(self):
		return hash('{}'.format(self.doc_id))