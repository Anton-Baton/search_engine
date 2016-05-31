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
				return json.load(f)
		self.inverted_index = load_json_from_file('inverted_index')
		self.forward_index = load_json_from_file('forward_index')
		self.url_to_id = load_json_from_file('url_to_id')

	# query [word1, word2] -> all documents that contains one of this words
	def find_documents(self, words):
		posting_list = []
		for word in words:
			# posting list [(pos, doc_id)]
			# TODO: check the situations when word does not in index
			posting_list.append(self.inverted_index[word])
		return posting_list

def create_index_from_dir(stored_documents_dir, index_dir):
	indexer = Indexer()
	for filename in os.listdir(stored_documents_dir):
		with open(os.path.join(stored_documents_dir, filename), 'r') as f:
			# TODO: word separated not just by spaces
			parsed_doc = parse_reddit_post(f.read()).split()
			indexer.add_document(base64.b16encode(filename), parsed_doc)
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