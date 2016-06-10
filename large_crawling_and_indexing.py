from crawler import Crawler
from indexer import create_index_from_dir
import time
import logging

def crawl_and_index():
	operation_start_time = time.time()
	logging.getLogger().setLevel(logging.DEBUG)
	crawling_start = time.time()
	crawler = Crawler('https://en.wikipedia.org/wiki/Milky_Way', 'wiki_10k_crawled_urls',
		15000)
	crawler.crawl_wikipedia()
	crawling_end = indexing_start = time.time()
	indexer = create_index_from_dir('wiki_10k_crawled_urls', 'wiki_10k_indices')
	indexing_blocks_end = time.time()
	indexer.save_on_disk('wiki_10k_indices')
	indexing_end = time.time()

	logging.debug('Crawling: {}'.format(crawling_end - crawling_start))
	logging.debug('Indexing bloks: {}'.format(indexing_blocks_end - indexing_start))
	logging.debug('Indexing whole: {}'.format(indexing_end - indexing_start))
	logging.debug('All: {}'.format(indexing_end - crawling_start))

if __name__ == '__main__':
	crawl_and_index()



	
