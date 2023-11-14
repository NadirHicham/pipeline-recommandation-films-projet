from pykafka import KafkaClient
import requests
import json

#Kafka Script
client = KafkaClient(hosts="localhost:9092")

topic = client.topics['movies_info']

producer = topic.get_sync_producer()

def get_response_movies(page):
    API_KEY = 'bba5904c90ac4619fc37e24c7067897a'
    MOVIE_ENDPOINT = "https://api.themoviedb.org/3/movie/popular?api_key={}&language=en-US&page={}"

    response = requests.get(MOVIE_ENDPOINT.format(API_KEY,page))

    return response

page = 1

if get_response_movies(page).status_code == 200:
    
    totalPages = get_response_movies(page).json()['total_pages']

    while page < totalPages:
        movies = get_response_movies(page).json()['results']

        for movie in movies:
            producer.produce(json.dumps(movie).encode('utf-8'))
        page = page +1
        
else:
    print("Error fetching data from TMDb API")