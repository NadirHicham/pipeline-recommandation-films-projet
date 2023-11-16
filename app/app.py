#import flask
import pandas as pd
import json

from datetime import datetime

from flask import Flask, render_template, request
from elasticsearch import Elasticsearch

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import random



app = Flask(__name__, template_folder='templates')


def get_es_records_df():
    es_host = 'http://127.0.0.1:9200'
    es = Elasticsearch(hosts=[es_host])
    index_name = 'movies-index'

    query = {
        "query": {
            "match_all": {}
        }
    }

    result = es.search(index=index_name, body=query, size=2000)
    redata = map(lambda x:x['_source'], result['hits']['hits'])

    return pd.DataFrame(redata)

df = get_es_records_df()

count = CountVectorizer(stop_words='english')
count_matrix = count.fit_transform(df['description'])

df = df.reset_index()

indices = pd.Series(df.index, index=df['original_title'])

def get_recommendations(title):
    cosine_sim = cosine_similarity(count_matrix, count_matrix)
    idx = indices[title]

    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:5] #1:11

    movie_indices = [i[0] for i in sim_scores]
    tit = df['original_title'].iloc[movie_indices]
    dat = df['release_date'].iloc[movie_indices]
    rating = df['vote_average'].iloc[movie_indices]
    description=df['description'].iloc[movie_indices]
    backdrop_path = df['backdrop_path'].iloc[movie_indices]
    poster_path = df['poster_path'].iloc[movie_indices]
    movieid=df['id'].iloc[movie_indices]


    return_df = pd.DataFrame(columns=['title','year'])
    return_df['title'] = tit
    return_df['year'] = pd.to_datetime(dat,unit='ms').dt.year
    return_df['ratings'] = rating
    return_df['description']=description
    return_df['id']=movieid
    return_df['backdropPath'] = 'https://image.tmdb.org/t/p/original/'+backdrop_path
    return_df['posterPath'] = 'https://image.tmdb.org/t/p/original/'+poster_path

    #current movie
    c_movie = df.loc[df['original_title'] == title]
    c_movie['backdrop_path'] = 'https://image.tmdb.org/t/p/original/'+df['backdrop_path']
    c_movie['poster_path'] = 'https://image.tmdb.org/t/p/original/'+df['poster_path']
    c_movie['release_date'] = pd.to_datetime(c_movie['release_date'],unit='ms').dt.year

    return json.loads(return_df.to_json(orient='records')), json.loads(c_movie.to_json(orient='records'))

def get_suggestions():
    return list(df['original_title'].str.capitalize())


@app.route("/")
@app.route("/index")
def index():
    
    suggestions = get_suggestions()
    return render_template('index.html',suggestions=suggestions)

@app.route('/recommandation', methods=['GET', 'POST'])

def main():
    if request.method == 'GET':
        return(render_template('index.html'))
    
    if request.method == 'POST':
        m_name = request.form['movie_name']
        m_name = m_name.title()

    rec = get_recommendations(m_name)

    return render_template('similar.html',m_name=rec[1],result=rec[0])


if __name__ == '__main__':
    app.run(debug=True)