import requests
from bs4 import BeautifulSoup

def download_url(url):
	#assert url.startswith('http://www.reddit.com/r/')
	headers = {
		'User-Agent': 'SearchingBot 0.1',
	}
	r = requests.get(url, headers=headers)
	if r.status_code != 200:
		raise Exception(r.status_code)
	return r.text
 

def parse_wiki_page(html):
	soup = BeautifulSoup(html)
	wiki_page_text = ' '.join(map(lambda x: x.text,
		soup.select('div#mv-content-text.mv-content-ltr p')))
	return wiki_page_text, 0

def parse_reddit_post(html):
	soup = BeautifulSoup(html)
	post = ' '.join(map(lambda x: x.text, 
		soup.select('div.content div.usertext-body')))
	score = int(soup.select('div.content div.score.unvoted')[0].text)
	#post = soup.select('div.usertext-body')[0].text + ' ' + ' '.join(map(lambda x: x.text, 
	#	soup.select('div.commentarea div.usertext-body')))
	return post, score