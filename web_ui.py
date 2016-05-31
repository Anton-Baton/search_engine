from flask import Flask, render_template, url_for, redirect
from flask_bootstrap import Bootstrap
from flask_wtf import Form
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from indexer import Searcher

app = Flask(__name__)
Bootstrap(app)
# TODO: configurable
searcher = Searcher('indeces')


class SearchForm(Form):
	user_query = StringField('user_query', validators=[DataRequired()])
	search_button = SubmitField('Search!')


@app.route("/", methods=['GET', 'POST'])
def index():
	search_form = SearchForm(csrf_enabled=False)
	if search_form.validate_on_submit():
		return redirect(url_for("search_results", query=search_form.user_query.data))
	return render_template('index.html', form=search_form)

@app.route("/search_results/<query>")
def search_results(query):
	pos_and_docids = searcher.find_documents(query.split())
	urls = [searcher.get_url(id) for pos, id in pos_and_docids]
	return render_template('search_results.html', query=query, urls=urls)

if __name__ == '__main__':
	app.run(debug=True)