import requests
from bs4 import BeautifulSoup

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