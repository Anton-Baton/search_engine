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


class Crawler(object):
	def __init__(self, start_url, storage_dir):
		self.start_url = start_url
		self.storage_dir = storage_dir

	@staticmethod
	def _make_absolute_url(url):
		return 'http://reddit.com' + url

	def crawl(self):
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
				if url.startswith('/wiki'):
					return True
			return False

		def make_absolute_wiki_url(url):
			return 'https://en.wikipedia.org' + url

		def prepare_url(url):
			return make_absolute_wiki_url(re.split(r'(#)|(File:)', a['href'])[0])		

		current_page_url = self.start_url
		logging.getLogger('requests').setLevel(logging.WARNING)
		logging.debug('Starting to crawl page {}'.format(self.start_url))

		#headers = {'User-Agent': 'SearchingBot 0.1'}	
		ok_url_count = 0
		error_url_count = 0
		links_to_crawl = deque()
		links_to_crawl.apped(current_page_url)
		crawled_links = set()
		while True:	
			url = links_to_crawl.popleft()
			if not url.startswith('https://en.wikipedia.org') or url in crawled_links:
				continue
			
			if (ok_url_count + error_url_count) % 100 == 0:
				logging.info("Crawled {} oks - {} errors".format(ok_url_count, error_url_count))
			try:
				current_page = download_url(url)
				logging.debug('200: {}'.format(url))
			except Exception as e:
				status_code = e.message
				logging.warning('{}: {}'.format(status_code, url))
				continue
			

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
			if ok_url_count >= 3000:
				break
			# time.sleep(2)

			


def main():
	logging.getLogger().setLevel(logging.DEBUG)
	parser = argparse.ArgumentParser(description='Crawl /r/astronomy/')
	parser.add_argument('--start_url',  dest='start_url', required=True)
	parser.add_argument('--storage_dir', dest='storage_dir', required=True)
	args = parser.parse_args()
	#print args.start_url
	crawler = Crawler(args.start_url, args.storage_dir)
	crawler.crawl_wikipedia()
	

if __name__ == '__main__':
	main()
