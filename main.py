import requests
from neo4j import GraphDatabase

url = "https://kinopoiskapiunofficial.tech/api/v2.2/films?order=RATING&ratingFrom=7&ratingTo=10&type=ALL&yearFrom=2000&yearTo=2010"
headers = {"X-API-KEY": "fe569b87-2d6c-4203-9b6a-edf8163e8ee9"}

uri = "bolt://localhost:7687"
username = "neo4j"
password = "password1"
driver = GraphDatabase.driver(uri, auth=(username, password))

def get_movies():
    response = requests.get(url, headers)
    all_movies = []
    page = 1
    while True:
        page_url = f"{url}&page={page}"
        response = requests.get(page_url, headers)
        if response.status_code == 200:
            movies_data = response.json().get("films", [])
            if not movies_data:
                break
            all_movies.extend(movies_data)
            page += 1
        else:
            break
    return all_movies


def create_movie_node(movie, session):
    head = (
        "MERGE (m:Movie {Id: $kinopoiskId}) "
        "ON CREATE SET "
        "m.title = $nameRu, "
        "m.rating = $ratingImdb, "
        "m.year = $year, "
        "m.countries = $countries, "
        "m.genres = $genres"
    )
    session.run(
        head,
        kinopoiskId=movie.get("kinopoiskId"),
        nameRu=movie.get("nameRu") if movie.get("nameRu") is not None else movie.get("nameEn"),
        ratingKinopoisk=movie.get("ratingKinopoisk"),
        year=movie.get("year"),
        type=movie.get("type"),
        countries=[country.get("country") for country in movie.get("countries")],
        genres=[genre.get("genre") for genre in movie.get("genres")]
    )
    session.close()
    driver.close()

def get_persons(kinopoisk_id):
    url = f"https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={kinopoisk_id}"
    response = requests.get(url, headers)
    persons = response.json()
    return persons

def create_relationship(kinopoisk_id, persons):
    session = driver.session()

    for person in persons:
        query = (
            "MERGE (p:Person {staffId: $staffId}) "
            "ON CREATE SET "
            "p.nameRu = $nameRu, "
            "p.professionKey = $professionKey"
        )
        session.run(
            query,
            staffId=person.get("staffId"),
            nameRu=person.get("nameRu"),
            professionKey=person.get("professionKey")
        )
        # Создание связей с фильмом на основе профессии
        relationship = {
            "DIRECTOR": "DIRECTED",
            "ACTOR": "ACTED_IN",
            "PRODUCER": "PRODUCED",
            "WRITER": "WROTE"
        }
        profession = person.get("professionKey")
        if profession in relationship:
            relation_type = relationship[profession]
            relation_query = (
                f"MATCH (m:Movie {{Id: $kinopoiskId}}), (p:Person {{staffId: $staffId}}) "
                f"MERGE (p)-[r:{relation_type}]->(m)"
            )
            session.run(relation_query, kinopoiskId=kinopoisk_id, staffId=person.get("staffId"))
    session.close()
    driver.close()

movies = get_movies()
for movie in movies:
    create_movie_node(movie)
    kinopoisk_id = movie.get("kinopoiskId")
    persons = get_persons(kinopoisk_id)
    create_relationship(kinopoisk_id, persons)