import requests
from bs4 import BeautifulSoup
import logging
import time
import os.path
import base64
import argparse
from util import download_url, parse_reddit_post
import re
from collections import deque
import time

class Crawler(object):
	def __init__(self, start_url, storage_dir, urls_to_crawl):
		self.start_url = start_url
		self.storage_dir = storage_dir
		self.urls_to_crawl = urls_to_crawl

	@staticmethod
	def _make_absolute_url(url):
		return 'http://reddit.com' + url

	def crawl_reddit(self):
		current_page_url = self.start_url
		logging.getLogger('requests').setLevel(logging.WARNING)
		logging.debug('Starting to crawl page {}'.format(self.start_url))

		#headers = {'User-Agent': 'SearchingBot 0.1'}	
		ok_url_count = 0
		error_url_count = 0
		while True:	
			
			if (ok_url_count + error_url_count) % 100 == 0:
				logging.info("Crawled {} oks - {} errors".format(ok_url_count, error_url_count))
			current_page = download_reddit_url(current_page_url)  # requests.get(current_page_url, headers=headers) 
			logging.debug('Current page: {}'.format(current_page_url))

			soup = BeautifulSoup(current_page)
			links = [Crawler._make_absolute_url(a['href']) for a in soup.find_all('a', attrs={'class': 'title'})
					if not (a['href'].startswith('http') or a['href'].startswith('javascript'))]
			try:	
				for link in links:
					ok_url_count += 1
					html = download_url(link)
					stored_text_file_name = os.path.join(self.storage_dir, base64.b16encode(link))
					with open(stored_text_file_name, 'w') as storage_file:
						storage_file.write(html.encode('utf-8'))
					time.sleep(2)
			except Exception as e:
				logging.error(u'Error occured while crawling {}'.format(current_page_url))
				logging.exception(e)
				error_url_count += 1

			next_page_url = soup.find('a', attrs={'rel': 'next'})['href']
			logging.debug('First post is {}'.format(links[0]))
			current_page_url = next_page_url
			ok_url_count += 1
			time.sleep(2)

	def crawl_wikipedia(self):

		def check_a_node(a):
			if a and a.get('href', None):
				url = a['href']
				ignore_urls_starts = ['/wiki/Wikipedia', '/wiki/Special', '/wiki/Category',
					'/wiki/Template_talk'
					'/wiki/Book', '/wiki/Template', '/wiki/Talk', '/wiki/BookSources', '/wiki/File']
				if url.startswith('/wiki') and not url.split(':')[0] in ignore_urls_starts:
					return True
			return False

		def make_absolute_wiki_url(url):
			return 'https://en.wikipedia.org' + url

		def prepare_url(url):
			return make_absolute_wiki_url(re.split(r'#', a['href'])[0])		

		start_time = time.time()
		current_page_url = self.start_url
		logging.getLogger('requests').setLevel(logging.WARNING)
		logging.debug('Starting to crawl page {}'.format(self.start_url))

		#headers = {'User-Agent': 'SearchingBot 0.1'}	
		ok_url_count = 0
		error_url_count = 0
		url_number = 0
		links_to_crawl = deque()
		links_to_crawl.append(current_page_url)
		crawled_links = set()
		while True:	
			url = links_to_crawl.popleft()
			if not url.startswith('https://en.wikipedia.org') or url in crawled_links:
				continue
			
			if (ok_url_count + error_url_count) % 100 == 0:
				logging.info("Crawled {} oks - {} errors".format(ok_url_count, error_url_count))
			try:
				current_page = download_url(url)
				logging.debug('{}. 200: {}'.format(url_number, url))
			except Exception as e:
				status_code = e.message
				logging.warning('{}. {}: {}'.format(url_number, status_code, url))
				continue
			url_number += 1

			soup = BeautifulSoup(current_page, 'html.parser')
			for tag in soup(['style', 'script']):
				tag.extract()

			links_to_crawl.extend(
				[prepare_url(a['href'])	for a in soup.find_all('a') if check_a_node(a)])
			try:					
				stored_text_file_name = os.path.join(self.storage_dir, base64.b16encode(url))
				with open(stored_text_file_name, 'w') as storage_file:
					storage_file.write(soup.get_text().encode('utf-8'))
				# time.sleep(2)
			except Exception as e:
				logging.error(u'Error occured while crawling {}'.format(current_page_url))
				logging.exception(e)
				error_url_count += 1
			ok_url_count += 1
			crawled_links.add(url)
			if ok_url_count >= self.urls_to_crawl:
				break
		logging.debug('Total time: {}'.format(time.time() - start_time))
			# time.sleep(2)			


def main():
	logging.getLogger().setLevel(logging.DEBUG)
	parser = argparse.ArgumentParser(description='Crawl /r/astronomy/')
	parser.add_argument('--start_url',  dest='start_url', required=True)
	parser.add_argument('--storage_dir', dest='storage_dir', required=True)
	parser.add_argument('--urls_count', dest='urls_count', type=int)
	args = parser.parse_args()
	#print args.start_url
	urls_to_crawl = args.urls_count if hasattr(args, 'urls_count') else 3000
	crawler = Crawler(args.start_url, args.storage_dir, urls_to_crawl)
	crawler.crawl_wikipedia()
	

if __name__ == '__main__':
	main()
