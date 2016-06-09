from nltk.stem import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import sent_tokenize, TreebankWordTokenizer
import itertools
import string


class Term(object):
	def __init__(self, full_word):
		self.full_word = full_word
		self.stem = PorterStemmer().stem(full_word).lower()

	def __eq__(self, other):
		return self.stem == other.stem

	def __hash__(self):
		return hash(self.stem)

	def is_punctuation(self):
		return self.stem in string.punctuation

	def __str__(self):
		return '{}({})'.format(self.stem, self.full_word)

	def __repr__(self):
		return str(self)


def stem_and_tokenize_text(text):
	sents = sent_tokenize(text)
	tokens = list(itertools.chain(*[TreebankWordTokenizer().tokenize(sent) 
		for sent in sents]))
	terms = [Term(token) for token in tokens]
	return [term for term in terms if not term.is_punctuation()]
 

def to_query_terms(query_raw):
	# In case query needs some processing
	return stem_and_tokenize_text(query_raw)


def to_doc_terms(doc_raw):
	# In case doc needs some processing
	return stem_and_tokenize_text(doc_raw)