#!/usr/bin/search_engine python
# Forward index
# Inderted index

# FI
# doc1 -> [learning, python, how, to]
# doc2 -> [learning, c++]
# doc3 -> [python, c++]

# ID
# learning -> [doc1, doc2]
# python -> [doc1]
# c++ -> [doc2]

# query: [learning, python]
# learning: [doc1, doc2]
# python: [doc1, doc3]

import pickle
import shelve
import os
import base64
import argparse
from util import parse_reddit_post
from collections import defaultdict
from lang_proc import to_doc_terms
import time
import math
from progressbar import ProgressBar, Bar


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


class ShelveIndeces(object):
	def __init__(self):
		self.inverted_index = None
		self.forward_index = None
		self.url_to_id = None
		self.id_to_url = dict()
		self.doc_count = 0

	def save_on_disk(self, index_dir):
		self.inverted_index.close()
		self.forward_index.close()
		self.url_to_id.close()

	def load_from_disk(self, index_dir):
		self.inverted_index = shelve.open(os.path.join(index_dir, 'inverted_index'))
		self.forward_index = shelve.open(os.path.join(index_dir, 'forward_index'))
		self.url_to_id = shelve.open(os.path.join(index_dir, 'url_to_id'))
		self.id_to_url = {v:k for k, v in self.url_to_id.iteritems()}
		self.doc_count = 0

	def start_indexing(self, index_dir):
		# 'c' - for append
		# 'n' - for rewrite
		self.inverted_index = shelve.open(os.path.join(index_dir, 'inverted_index'), 'n')
		self.forward_index = shelve.open(os.path.join(index_dir, 'forward_index'), 'n')
		self.url_to_id = shelve.open(os.path.join(index_dir, 'url_to_id'), 'n')

	def add_document(self, url, document):
		self.doc_count += 1

		if url in self.url_to_id:
			print url
			return

		current_id = self.doc_count
		self.url_to_id[url] = current_id
		self.id_to_url[str(current_id)] = url
		self.forward_index[str(current_id)] = document

		for pos, term in enumerate(document.parsed_text):
			stem = term.stem.encode('utf-8')
			posts = self.inverted_index.get(stem, []) 
			posts.append((pos, current_id))
			self.inverted_index[stem] = posts

	def get_documents(self, query_term): 
		return self.inverted_index.get(query_term.stem.encode('utf-8'), [])

	def get_document_text(self, doc_id):
		return self.forward_index[str(doc_id)].parsed_text

	def get_url(self, doc_id):
		return self.id_to_url[doc_id]

	def get_document_score(self, doc_id):
		return self.forward_index[str(doc_id)].score


class SearchResults(object):
	def __init__(self, docids_with_relevance):
		self.docids, self.relevance = zip(*docids_with_relevance)

	def get_page(self, page, page_size):
		offset = (page-1)*page_size
		return self.docids[offset: offset+page_size]

	def total_pages(self, page_size):
		return int(math.ceil(len(self.docids)*1.0/page_size))

	def total_docs(self):
		return len(self.docids)


class Searcher(object):
	def __init__(self, index_dir, IndecesImplementation):
		self.indeces = IndecesImplementation()
		self.indeces.load_from_disk(index_dir)

	# query [word1, word2] -> all documents that contains one of this words
	# OR-LIKE
	def find_documents_OR(self, query_terms, offset=None, limit=None):
		docids = set()
		for query_term in query_terms:
		 	for (pos, doc_id) in self.indeces.get_documents(query_term):
		 		docids.add(doc_id)
		return SearchResults(self.rank_docids(docids))
	
	def rank_docids(self, docids):
		return sorted([(doc_id, self.indeces.get_document_score(doc_id)) for doc_id in docids], key=lambda x: x[1])

	# AND-LIKE - if all words in doc
	def find_documents_AND(self, query_terms, offset=None, limit=None):
		query_terms_count = defaultdict(set)
		for query_term in query_terms:
			for pos, doc_id in self.indeces.get_documents(query_term):
				query_terms_count[doc_id].add(query_term)
		return SearchResults(self.rank_docids([doc_id for doc_id, unique_hits in query_terms_count.iteritems() 
				if len(unique_hits) == len(query_terms)]))

	def generate_snippet(self, query_terms, doc_id): 
		query_terms_in_window = []
		best_window_len = 10**8
		best_window = []
		terms_in_best_window = 0
		document = self.indeces.get_document_text(doc_id)
		start_time = time.time()
		for pos, term in enumerate(document):
			if term in query_terms:
				query_terms_in_window.append((term, pos))

				if len(query_terms_in_window) > 1 and query_terms_in_window[0][0] == term:
					query_terms_in_window.pop(0)
				current_window_len = pos - query_terms_in_window[0][1] + 1
				tiw = len(set(map(lambda x: x[0], query_terms_in_window)))
				if tiw > terms_in_best_window or (tiw == terms_in_best_window 
					and current_window_len < best_window_len):
					best_window = query_terms_in_window[:]
					best_window_len = current_window_len
					terms_in_best_window = tiw
		#print 'Snippet time: ', time.time() - start_time
		doc_len = len(document)
		# TODO: move 15 to named constants
		snippet_start = max(best_window[0][1] - 15, 0)
		snippet_end = min(doc_len, best_window[-1][1] + 1 +15)
		return [(term.full_word, term in query_terms) for term in document[snippet_start: snippet_end]]

	def get_url(self, doc_id):
		return self.indeces.get_url(doc_id)


def create_index_from_dir(stored_documents_dir, index_dir,
	IndecesImplementation=ShelveIndeces):

	indexer = IndecesImplementation()
	indexer.start_indexing(index_dir)
	total_docs_indexed = 0
	total_docs = len(os.listdir(stored_documents_dir))
	for filename in os.listdir(stored_documents_dir):
		with open(os.path.join(stored_documents_dir, filename), 'r') as f:
			doc_raw, doc_score = parse_reddit_post(f.read())
			parsed_doc = to_doc_terms(doc_raw)
			indexer.add_document(base64.b16decode(filename), Document(parsed_doc, doc_score))
			total_docs_indexed += 1
			if total_docs_indexed % 100 == 0:
				print 'Indexed: ', total_docs_indexed
	return indexer


def main():
	#logging.getLogger().setLevel(logging.INFO)
	parser = argparse.ArgumentParser(description='Index /r/astronomy/')
	parser.add_argument('--stored_documents_dir',  dest='stored_documents_dir', required=True)
	parser.add_argument('--index_dir', dest='index_dir', required=True)
	args = parser.parse_args()
	#print args.start_url
	indexer = create_index_from_dir(args.stored_documents_dir, args.index_dir)
	indexer.save_on_disk(args.index_dir)
	
	

if __name__ == '__main__':
	main()