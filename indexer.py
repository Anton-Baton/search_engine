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

import json
import os
import base64
import argparse
from util import parse_reddit_post
from collections import defaultdict

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
	# parsed text is list of words
	def add_document(self, url, parsed_text):
		self.doc_count += 1
		assert url not in self.url_to_id
		current_id = self.doc_count
		self.url_to_id[url] = current_id
		self.forward_index[current_id] = parsed_text
		for position, word in enumerate(parsed_text):
			# TODO: defaultdict
			if word not in self.inverted_index:
				self.inverted_index[word] = []
			self.inverted_index[word].append((position, current_id))
	

	def save_on_disk(self, index_dir):

		def dump_json_to_file(source, file_name):
			file_path = os.path.join(index_dir, file_name)
			with open(file_path, 'w') as f:
				json.dump(source, f, indent=4)
		dump_json_to_file(self.inverted_index, 'inverted_index')
		dump_json_to_file(self.forward_index, 'forward_index')
		dump_json_to_file(self.url_to_id, 'url_to_id')


class Searcher(object):
	def __init__(self, index_dir):
		def load_json_from_file(file_name):
			file_path = os.path.join(index_dir, file_name)
			with open(file_path, 'r') as f:
				# TODO: is it correct to return immediatly ?
				return json.load(f)
		self.inverted_index = load_json_from_file('inverted_index')
		self.forward_index = load_json_from_file('forward_index')
		self.url_to_id = load_json_from_file('url_to_id')

		self.id_to_url = {v: k for k, v in self.url_to_id.iteritems()}
	
	# query [word1, word2] -> all documents that contains one of this words
	# OR-LIKE
	def find_documents_OR(self, query_words):
		docids = set()
		for query_word in query_words:
		 	# posting list [(pos, doc_id)]
		 	# TODO: check the situations when word does not in index
		 	#posting_list.extend(self.inverted_index[word])
		 	for (pos, doc_id) in self.inverted_index[query_word]:
		 		docids.add(doc_id)
		return docids
		#return sum([self.inverted_index[word] for word in query_words], [])
	
	# AND-LIKE - if all words in doc
	def find_documents_AND(self, query_words):
		query_words_count = defaultdict(set)
		for word in query_words:
			for pos, doc_id in self.inverted_index[word]:
				query_words_count[doc_id].add(word)
		return [doc_id for doc_id, unique_hits in query_words_count.iteritems() 
				if len(unique_hits) == len(query_words)]

	def generate_snippet(self, query_words, doc_id): 
		query_words_in_window = []
		best_window_len = 10**8
		best_window = []
		words_in_best_window = 0
		# TODO: fix doc_id is string
		for pos, word in enumerate(self.forward_index[str(doc_id)]):
			if word in query_words:
				query_words_in_window.append((word, pos))

				if len(query_words_in_window) > 1 and query_words_in_window[0][0] == word:
					query_words_in_window.pop(0)
				current_window_len = pos - query_words_in_window[0][1] + 1
				wiw = len(set(map(lambda x: x[0], query_words_in_window)))
				if wiw > words_in_best_window or (wiw == words_in_best_window 
					and current_window_len < best_window_len):
					best_window = query_words_in_window[:]
					best_window_len = current_window_len
					words_in_best_window = wiw
		doc_len = len(self.forward_index[str(doc_id)])
		# TODO: move 15 to named constants
		snippet_start = max(best_window[0][1] - 15, 0)
		snippet_end = min(doc_len, best_window[-1][1] + 1 +15)
		return [(word, word in query_words) for word in self.forward_index[str(doc_id)][snippet_start: snippet_end]]


	def get_document_text(self, doc_id):
		# TODO: fix str - something strange
		return self.forward_index[str(doc_id)]

	def get_url(self, doc_id):
		return self.id_to_url[doc_id]

def create_index_from_dir(stored_documents_dir, index_dir):
	indexer = Indexer()
	for filename in os.listdir(stored_documents_dir):
		with open(os.path.join(stored_documents_dir, filename), 'r') as f:
			# TODO: word separated not just by spaces
			parsed_doc = parse_reddit_post(f.read()).split()
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