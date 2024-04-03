from flask import Flask
import pymysql
import getpass
from flask import request 
import math
from collections import defaultdict
from flask_basicauth import BasicAuth
import json
import cryptography


MAX_PAGE_SIZE = 100

app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

@app.route("/movies/<int:movie_id>")
@auth.required

def movie(movie_id):
    def remove_nulls(obj):
      return{k:v for k,v in obj.items() if v is not None}
    passw = "19Klipsch46"
    db_conn= pymysql.connect(host="localhost", user="root", password=passw, database="bechdel", cursorclass=pymysql.cursors.DictCursor)
    
    
    with db_conn.cursor() as cursor:
            cursor.execute("""select M.movieId, M.originalTitle as Title, M.primaryTitle as englishTitle, B.rating, M.startYear as year, M.runtimeMinutes from Movies as M 
                           join Bechdel as B 
                           on B.movieId=M.movieId 
                           where M.movieId=%s""", (movie_id, ))
            movie = cursor.fetchone()
            movie = remove_nulls(movie)

    with db_conn.cursor() as cursor:
          cursor.execute("""Select * from MoviesGenres where movieId=%s""", (movie_id, ))
          genres=cursor.fetchall()
    movie['genres']=[g['genre']for g in genres]


    with db_conn.cursor() as cursor:
        cursor.execute("""Select p.personId, p.primaryName as name, p.birthYear, p.deathYear, mp.job, mp.category from MoviesPeople as mp join People as p on p.personID =mp.personID where movieId=%s""", (movie_id, ))
        people=cursor.fetchall()
    movie['people']= people
    movie['people']= [remove_nulls(p) for p in people]

    db_conn.close()
    return movie

@app.route("/movies")
@auth.required
def movies():
    
    def remove_nulls(obj):
      return{k:v for k,v in obj.items() if v is not None}
    
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details', 0)))

    
    passw = "19Klipsch46"
    db_conn = pymysql.connect(host="localhost", user="root", database="bechdel", password=passw,cursorclass=pymysql.cursors.DictCursor)
    
    
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT distinct M.movieId, M.originalTitle as Title, M.primaryTitle as englishTitle, B.rating, M.startYear as year, M.runtimeMinutes from Movies as M
            left join MoviesGenres mg on m.movieId = mg.movieId
            left join Bechdel as b on m.movieId = b.movieId
            ORDER BY m.movieId
            LIMIT %s
            OFFSET %s
        """, (page_size, (page-1)*page_size))
        movies = cursor.fetchall()


         # list of movie IDs
        movie_ids = [m['movieId'] for m in movies]
        if include_details:
        #  genres for each movie
            genres_by_movie = defaultdict(list)

            for movie_id in movie_ids:
                with db_conn.cursor() as cursor:
                    cursor.execute("SELECT genre FROM MoviesGenres WHERE movieId = %s", (movie_id,))
                    genres = cursor.fetchall()
                    genres_by_movie[movie_id] = [genre['genre'] for genre in genres]
            
            people_by_movie = defaultdict(list)

            for movie_id in movie_ids:
                with db_conn.cursor() as cursor:
                    cursor.execute("""Select p.personId, p.primaryName as name, p.birthYear, p.deathYear, mp.job, mp.category from MoviesPeople as mp join People as p on p.personID =mp.personID where movieId=%s""", (movie_id, ))
                    people=cursor.fetchall()
                    people_by_movie[movie_id] = [remove_nulls(person) for person in people]
            
            
            for movie in movies:
                movie_id = movie['movieId']
                movie['genres'] = genres_by_movie.get(movie_id, [])
                movie['people'] = people_by_movie.get(movie_id,[])

        with db_conn.cursor() as cursor:
            cursor.execute("Select count(*) as total from Movies")
            total = cursor.fetchone()
            last_page=math.ceil(total['total']/ page_size)

         
    db_conn.close()
    return {'movies': movies,
            'next_page': f"/movies?page={page+1}&page_size={page_size}&include_details={int(include_details)}",
            'last_page': f"/movies?page={last_page}&page_size={page_size}&include_details={int(include_details)}",}      