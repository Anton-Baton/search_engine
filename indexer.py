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
from lang_proc import to_doc_terms, Term
import time
import math
import workaround
import logging


class ShelveIndeces(object):
	def __init__(self):
		self.inverted_index = None
		self.forward_index = None
		self.url_to_id = None
		self.id_to_url = dict()
		self.doc_count = 0
		self.block_count = 0

	def save_on_disk(self, index_dir):
		self.inverted_index.close()
		self.forward_index.close()
		self.url_to_id.close()
		self._merge_blocks()

	def load_from_disk(self, index_dir):
		self.inverted_index = shelve.open(os.path.join(index_dir, 'inverted_index'), writeback=True)
		self.forward_index = shelve.open(os.path.join(index_dir, 'forward_index'), writeback=True)
		self.url_to_id = shelve.open(os.path.join(index_dir, 'url_to_id'), writeback=True)
		self.id_to_url = {v:k for k, v in self.url_to_id.iteritems()}
		self.doc_count = 0

	def start_indexing(self, index_dir):
		# 'c' - for append
		# 'n' - for rewrite
		# self.inverted_index = shelve.open(os.path.join(index_dir, 'inverted_index'), 'n', writeback=True)
		self.forward_index = shelve.open(os.path.join(index_dir, 'forward_index'), 'n', writeback=True)
		self.url_to_id = shelve.open(os.path.join(index_dir, 'url_to_id'), 'n', writeback=True)
		self.index_dir = index_dir

	def sync(self):
		self.inverted_index.sync()
		self.forward_index.sync()
		self.url_to_id.sync()

	def _merge_blocks(self):
		logging.debug('Start merging blocks')
		blocks = [shelve.open(os.path.join(self.index_dir, 'inverted_index_block{}'.format(i))) for i in xrange(self.block_count+1)]
		keys = set()  #sum([set(block.keys()) for block in blocks], set)
		for block in blocks:
			keys |= set(block.keys())
		logging.debug('Total keys: {}'.format(len(keys)))
		merged_index = shelve.open(os.path.join(self.index_dir, 'inverted_index'), 'n')
		for key in keys:
			merged_index[key] = sum([block.get(key, []) for block in blocks], [])
		merged_index.close()

	def _create_new_ii_block(self):
		logging.debug('New ii block: {}'.format(self.block_count))
		if self.inverted_index:
			self.inverted_index.close() 
		self.inverted_index = shelve.open(
				os.path.join(self.index_dir, 'inverted_index_block{}'.format(
					self.block_count)), 'n', writeback=True)
		logging.debug('Block created!')
		self.block_count += 1
		
	def add_document(self, url, document):
		if self.doc_count % 200 == 0:
			self._create_new_ii_block()

		self.doc_count += 1

		if url in self.url_to_id:
			logging.debug('URL already indexed: {}'.format(url))
			return

		current_id = self.doc_count
		self.url_to_id[url] = current_id
		self.id_to_url[str(current_id)] = url
		self.forward_index[str(current_id)] = document

		for pos, term in enumerate(document.parsed_text):
			stem = term.stem.encode('utf-8') 
			if stem not in self.inverted_index:
				self.inverted_index[stem] = []
			self.inverted_index[stem].append(workaround.InvertedIndexHit(current_id, pos, document.score))
			#self.inverted_index[stem].append((pos, current_id))

	def get_documents(self, query_term): 
		return self.inverted_index.get(query_term.stem.encode('utf-8'), [])

	def get_document_text(self, doc_id):
		return self.forward_index[str(doc_id)].parsed_text

	def get_document_score(self, doc_id):
		return self.forward_index[str(doc_id)].score

	def get_url(self, doc_id):
		return self.id_to_url[doc_id]


class SearchResults(object):
	def __init__(self, docids_with_relevance):
		self.docids, self.relevance = zip(*docids_with_relevance) if docids_with_relevance else ([], [])

	def get_page(self, page, page_size):
		offset = (page-1)*page_size
		return self.docids[offset: offset+page_size]

	def total_pages(self, page_size):
		return int((len(self.docids)+page_size)*1.0/page_size)

	def total_docs(self):
		return len(self.docids)


class Searcher(object):
	def __init__(self, index_dir, IndecesImplementation):
		self.indeces = IndecesImplementation()
		self.indeces.load_from_disk(index_dir)

	# query [word1, word2] -> all documents that contains one of this words
	# OR-LIKE
	def find_documents_OR(self, query_terms, offset=None, limit=None):
		docids_and_relevances = set()
		for query_term in query_terms:
		 	for hit in self.indeces.get_documents(query_term):
		 		docids_and_relevances.add((hit.doc_id, hit.score))
		 		#docids_and_relevances.add((doc_id, self.indeces.get_document_score(doc_id)))
 		return SearchResults(sorted(list(docids_and_relevances), key=lambda x: x[1], reverse=True))
	"""
	# AND-LIKE - if all words in doc
	def find_documents_AND(self, query_terms, offset=None, limit=None):
		query_terms_count = defaultdict(set)
		for query_term in query_terms:
			for pos, doc_id in self.indeces.get_documents(query_term):
				query_terms_count[doc_id].add(query_term)
		return SearchResults(self.rank_docids([doc_id for doc_id, unique_hits in query_terms_count.iteritems() 
				if len(unique_hits) == len(query_terms)]))
	"""

	def generate_snippet(self, query_terms, doc_id):
		# TODO: move constants to config
		snippet_max_len = 50
		snippet_padding = 15
		query_terms_in_window = []
		best_window_len = 10**8
		best_window = []
		terms_in_best_window = 0
		document = self.indeces.get_document_text(doc_id)
		start_time = time.time()
		print query_terms
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
		doc_len = len(document)
		snippet_start = max(best_window[0][1] - snippet_padding, 0)
		snippet_end = min(doc_len, best_window[-1][1] + 1 + snippet_padding)
		snippet_len = snippet_end - snippet_start
		if snippet_len > snippet_max_len:
			sub_windows = []
			for i in xrange(len(best_window)-1):
				left, right = best_window[i], best_window[i+1]
				sub_windows.append((left[1], right[1], right[1] - left[1]))
			# sort in descending order by window width
			sub_windows_sorted = sorted(sub_windows, key=lambda x: x[2], reverse=True)
			delta = snippet_len - snippet_max_len
			pointer = 0
			zip_coordinates = set()
			ellipsis_term = Term('...')
			while delta > 0:
				w_start, w_end, w_len = sub_windows_sorted[pointer]
				delta -= w_len
				zip_coordinates.add(w_start)
				pointer += 1
			snippet = [term for term in document[snippet_start: best_window[0][1]]]
			for l, r, d in sub_windows:
				snippet.append(document[l])
				if l in zip_coordinates:
					snippet.append(ellipsis_term)
				else:
					snippet.append(Term(' '.join(map(lambda x: x.full_word, document[l:r]))))
			snippet.extend([term for term in document[best_window[-1][1]:snippet_end]])
			return [(term.full_word, term in query_terms) for term in snippet]
		return [(term.full_word, term in query_terms) for term in document[snippet_start: snippet_end]]

	def get_url(self, doc_id):
		return self.indeces.get_url(doc_id)


def create_index_from_dir(stored_documents_dir, index_dir,
	IndecesImplementation=ShelveIndeces):

	indexer = IndecesImplementation()
	indexer.start_indexing(index_dir)
	total_docs_indexed = 0
	#total_docs = len(os.listdir(stored_documents_dir))
	for filename in os.listdir(stored_documents_dir):
		with open(os.path.join(stored_documents_dir, filename), 'r') as f:
			# TODO: Swith, cause reddit and wiki parsed with differences
			#doc_raw, doc_score = parse_reddit_post(f.read())
			doc_raw = f.read().decode('utf-8')
			doc_score = 0
			parsed_doc = to_doc_terms(doc_raw)
			indexer.add_document(base64.b16decode(filename), workaround.Document(parsed_doc, doc_score))
			total_docs_indexed += 1
			logging.debug('Doc num: {}'.format(total_docs_indexed))
			if total_docs_indexed % 100 == 0:
				#print 'Indexed: ', total_docs_indexed
				logging.debug('Sync...')
				indexer.sync()
				logging.debug('Sync!')
	return indexer


def main():
	logging.getLogger().setLevel(logging.DEBUG)
	parser = argparse.ArgumentParser(description='Index /r/astronomy/')
	parser.add_argument('--stored_documents_dir',  dest='stored_documents_dir', required=True)
	parser.add_argument('--index_dir', dest='index_dir', required=True)
	args = parser.parse_args()
	#print args.start_url
	indexer = create_index_from_dir(args.stored_documents_dir, args.index_dir)
	indexer.save_on_disk(args.index_dir)
	
	

if __name__ == '__main__':
	main()