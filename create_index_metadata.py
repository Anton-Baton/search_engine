import shelve
import os
from workaround import Document
from indexer import ShelveIndeces
import logging
import time

def create_metadata(index_dir):
	start_time = time.time()
	logging.debug('Start creating metadata')
	forward_index = shelve.open(os.path.join(index_dir, 'forward_index'))
	logging.debug('Opened forward index')
	fi_open_time = time.time()
	docs_count = 0
	total_words_count = 0
	for doc_id, document in forward_index.iteritems():
		if docs_count % 200 == 0:
			logging.debug('Done: {} | Words: {}'.format(docs_count, total_words_count))
		docs_count += 1
		total_words_count += len(document.parsed_text)
	cd_time = time.time()
	logging.debug('Done calculations!')
	forward_index.close()
	index_metadata_shelf = shelve.open(os.path.join(index_dir, 'index_metadata'), 'n', writeback=True)
	index_metadata_shelf['documents_count'] = docs_count
	index_metadata_shelf['total_words_count'] = total_words_count
	index_metadata_shelf.close()
	end_time = time.time()
	logging.debug('Saved!')
	logging.debug('FI open time: {} | Calc time: {} | Total time:'.format(
		fi_open_time-start_time, cd_time-fi_open_time, end_time-start_time))

if __name__ == '__main__':
	logging.getLogger().setLevel(logging.DEBUG)
	create_metadata('wiki_10k_indices')


