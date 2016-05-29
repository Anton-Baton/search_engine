import requests
from bs4 import BeautifulSoup
import logging
import time
import os.path
import base64

# TODO: is it good?
logging.getLogger().setLevel(logging.DEBUG)

def download_reddit_url(url):
	#assert url.startswith('http://www.reddit.com/r/')
	headers = {
		'User-Agent': 'SearchingBot 0.1',
	}
	r = requests.get(url, headers=headers)
	if r.status_code != 200:
		raise Exception('Non-OK status code: {}'.format(r.status_code))
	return r.text


def parse_reddit_post(html):
	soup = BeautifulSoup(html)
	post = soup.select('div.usertext-body')[1].text
	return post


class Crawler(object):
	def __init__(self, start_url, storage_dir):
		self.start_url = start_url
		self.storage_dir = storage_dir

	@staticmethod
	def _make_absolute_url(url):
		return 'http://reddit.com' + url

	def crawl(self):
		current_page_url = self.start_url
		logging.debug('Starting to crawl page {}'.format(self.start_url))

		#headers = {'User-Agent': 'SearchingBot 0.1'}	
		while True:	
			current_page = download_reddit_url(current_page_url)  # requests.get(current_page_url, headers=headers) 
			logging.debug('Current page: {}'.format(current_page_url))

			soup = BeautifulSoup(current_page)
			links = [Crawler._make_absolute_url(a['href']) for a in soup.find_all('a', attrs={'class': 'title'})
					if not (a['href'].startswith('http') or a['href'].startswith('javascript'))]
			for link in links:
				html = download_reddit_url(link)
				stored_text_file_name = os.path.join(self.storage_dir, base64.b16encode(link))
				with open(stored_text_file_name, 'w') as storage_file:
					storage_file.write(html.encode('utf-8'))
				time.sleep(2)

			next_page_url = soup.find('a', attrs={'rel': 'next'})['href']
			logging.debug('First post is {}'.format(links[0]))
			current_page_url = next_page_url
			time.sleep(2)



