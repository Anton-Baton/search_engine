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
	post = ' '.join(map(lambda x: x.text, 
		soup.select('div.content div.usertext-body')))
	score = soup.select('div.content div.score.unvoted')[0].text
	#post = soup.select('div.usertext-body')[0].text + ' ' + ' '.join(map(lambda x: x.text, 
	#	soup.select('div.commentarea div.usertext-body')))
	return post, score