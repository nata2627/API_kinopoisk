import requests
from neo4j import GraphDatabase

url = "https://kinopoiskapiunofficial.tech/api/v2.2/films?order=RATING&ratingFrom=7&ratingTo=10&type=ALL&yearFrom=2000&yearTo=2010"
headers = {"X-API-KEY": "fe569b87-2d6c-4203-9b6a-edf8163e8ee9"}

uri = "bolt://localhost:7687"
username = "neo4j"
password = "mypassword"
driver = GraphDatabase.driver(uri, auth=(username, password))

def get_movies():
    all_movies = []
    page = 1
    while True:
        page_url = f"{url}&page={page}"
        response = requests.get(page_url, headers=headers)
        if response.status_code == 200:
            movies_data = response.json().get("items")
            if not movies_data:
                break
            all_movies.extend(movies_data)
            page += 1
        else:
            break
    return all_movies

def create_movie_node(movie):
    session = driver.session()
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
        nameRu=movie.get("nameRu"),
        ratingImdb=movie.get("ratingImdb"),
        year=movie.get("year"),
        countries=[country.get("country") for country in movie.get("countries")],
        genres=[genre.get("genre") for genre in movie.get("genres")]
    )

def get_persons(kinopoisk_id):
    url = f"https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={kinopoisk_id}"
    response = requests.get(url, headers=headers)
    persons = response.json()
    return persons

def create_relationship(kinopoisk_id, persons):
    session = driver.session()

    for person in persons:
        head = (
            "MERGE (p:Person {staffId: $staffId}) "
            "ON CREATE SET "
            "p.nameRu = $nameRu, "
            "p.professionKey = $professionKey"
        )
        session.run(
            head,
            staffId=person.get("staffId"),
            nameRu=person.get("nameRu"),
            professionKey=person.get("professionKey")
        )

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

movies = get_movies()
for movie in movies:
    create_movie_node(movie)
    kinopoisk_id = movie.get("kinopoiskId")
    persons = get_persons(kinopoisk_id)
    create_relationship(kinopoisk_id, persons)

driver.close()