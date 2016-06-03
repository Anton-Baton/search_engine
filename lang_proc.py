from nltk.stem import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import sent_tokenize, TreebankWordTokenizer
import itertools
import string

def stem_and_tokenize_text(text):
	sents = sent_tokenize(text)
	tokens = list(itertools.chain(*[TreebankWordTokenizer().tokenize(sent) 
		for sent in sents]))
	stems = [PorterStemmer().stem(token) for token in tokens]
	terms = [stem.lower() for stem in stems if stem not in string.punctuation]
	return terms
 
def query_terms(query_raw):
	# In case query needs some processing
	return stem_and_tokenize_text(query_raw)

def doc_terms(doc_raw):
	# In case doc needs some processing
	return stem_and_tokenize_text(doc_raw)