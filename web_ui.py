from flask import Flask, render_template, url_for, redirect, g
from flask_bootstrap import Bootstrap
from flask_wtf import Form
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from indexer import Searcher, ShelveIndeces
import workaround
from workaround import Document
from lang_proc import to_query_terms
import time
import cgi
import os
import logging

app = Flask(__name__)
Bootstrap(app)
INDEX_DIR = 'wiki_10k_indices'
# TODO: configurable
#searcher = Searcher(os.environ['INDEX_DIR'], ShelveIndeces)
searcher = Searcher(INDEX_DIR, ShelveIndeces)


#@app.before_request
#def init_searcher():
#	start_time = time.time()
#	# TODO: move folder to config
#	g.searcher = Searcher('shelve_indeces', ShelveIndeces)
#	print 'Searcher time: ', time.time() - start_time

class SearchForm(Form):
	user_query = StringField('user_query', validators=[DataRequired()])
	search_button = SubmitField('Search!')


@app.route("/", methods=  ['GET', 'POST'])
def index():
	search_form = SearchForm(csrf_enabled=False)
	if search_form.validate_on_submit():
		return redirect(url_for("search_results", query=search_form.user_query.data))
	return render_template('index.html', form=search_form)


@app.route("/search_results/<query>", defaults={'page':1})
@app.route("/search_results/<query>/<int:page>")
def search_results(query, page):
	start_processing_time = time.time()
	query_terms = to_query_terms(query)
	page_size = 25
	offset = (page-1)*page_size

	start_search_time = time.time()
	search_results = searcher.find_documents_and_rank_by_bm25(query_terms)
	docids = search_results.get_page(page, page_size)
	logging.debug('Search time: {}'.format(time.time() - start_search_time))

	urls = [searcher.get_url(doc_id) for doc_id in docids]
	start_snippets_time = time.time()
	texts = [searcher.generate_snippet(query_terms, doc_id) for doc_id in docids]
	logging.debug('Snippets time: {}'.format(time.time() -start_snippets_time))
	
	full_time = time.time() - start_processing_time
	logging.debug('Full query processing time: {}'.format(full_time))
	return render_template('search_results.html',
		page=page, offset=offset+1, total_pages_num=search_results.total_pages(page_size),
		query=cgi.escape(query), total_docs_num=search_results.total_docs(),
		processing_time = full_time, urls_and_texts=zip(urls, texts))

if __name__ == '__main__':
	logging.getLogger().setLevel(logging.DEBUG)
	app.run(debug=True)