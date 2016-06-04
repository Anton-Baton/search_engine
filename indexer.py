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
import os
import base64
import argparse
from util import parse_reddit_post
from collections import defaultdict
from lang_proc import to_doc_terms


# TODO: improve
# Indexer asumes that collection fits in RAM
class Indexer(object):
	def __init__(self):
		self.inverted_index = dict()
		self.forward_index = dict()
		self.url_to_id = dict()
		self.doc_count = 0

	# TODO: remove assumptions
	# assume that add_document() never called twice for one doc
	# assumes that a documnet has an unique url
	# parsed text is list of Terms
	def add_document(self, url, parsed_text):
		self.doc_count += 1
		assert url not in self.url_to_id
		current_id = self.doc_count
		self.url_to_id[url] = current_id
		self.forward_index[current_id] = parsed_text
		for position, term in enumerate(parsed_text):
			# TODO: defaultdict
			if term not in self.inverted_index:
				self.inverted_index[term] = []
			self.inverted_index[term].append((position, current_id))
	

	def save_on_disk(self, index_dir):

		def dump_pickle_to_file(source, file_name):
			file_path = os.path.join(index_dir, file_name)
			with open(file_path, 'w') as f:
				pickle.dump(source, f)
		dump_pickle_to_file(self.inverted_index, 'inverted_index')
		dump_pickle_to_file(self.forward_index, 'forward_index')
		dump_pickle_to_file(self.url_to_id, 'url_to_id')


class Searcher(object):
	def __init__(self, index_dir):
		def load_pickle_from_file(file_name):
			file_path = os.path.join(index_dir, file_name)
			with open(file_path, 'r') as f:
				# TODO: is it correct to return immediatly ?
				return pickle.load(f)
		self.inverted_index = load_pickle_from_file('inverted_index')
		# need to do this, cause keys in pickle are always strings - but we need ints
		self.forward_index = load_pickle_from_file('forward_index')
		self.url_to_id = load_pickle_from_file('url_to_id')

		self.id_to_url = {v: k for k, v in self.url_to_id.iteritems()}
	
	# query [word1, word2] -> all documents that contains one of this words
	# OR-LIKE
	def find_documents_OR(self, query_terms):
		docids = set()
		for query_term in query_terms:
		 	if query_term in self.inverted_index:
			 	for (pos, doc_id) in self.inverted_index[query_term]:
			 		docids.add(doc_id)
		return docids
	
	# AND-LIKE - if all words in doc
	def find_documents_AND(self, query_terms):
		query_terms_count = defaultdict(set)
		for term in query_terms:
			if term in self.inverted_index:
				for pos, doc_id in self.inverted_index[term]:
					query_terms_count[doc_id].add(term)
		return [doc_id for doc_id, unique_hits in query_terms_count.iteritems() 
				if len(unique_hits) == len(query_terms)]

	def generate_snippet(self, query_terms, doc_id): 
		query_terms_in_window = []
		best_window_len = 10**8
		best_window = []
		terms_in_best_window = 0
		for pos, term in enumerate(self.forward_index[doc_id]):
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
		doc_len = len(self.forward_index[doc_id])
		# TODO: move 15 to named constants
		snippet_start = max(best_window[0][1] - 15, 0)
		snippet_end = min(doc_len, best_window[-1][1] + 1 +15)
		return [(term.full_word, term in query_terms) for term in self.forward_index[doc_id][snippet_start: snippet_end]]


	def get_document_text(self, doc_id):
		return self.forward_index[doc_id]

	def get_url(self, doc_id):
		return self.id_to_url[doc_id]

def create_index_from_dir(stored_documents_dir, index_dir):
	indexer = Indexer()
	for filename in os.listdir(stored_documents_dir):
		with open(os.path.join(stored_documents_dir, filename), 'r') as f:
			doc_raw = parse_reddit_post(f.read())
			parsed_doc = to_doc_terms(doc_raw)
			indexer.add_document(base64.b16decode(filename), parsed_doc)
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