from flask import Flask
import pymysql
import os
from flask import request 
import math
from collections import defaultdict
from flask_basicauth import BasicAuth
import json
import cryptography
from flask_swagger_ui import get_swaggerui_blueprint
from flask import abort

swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi.yaml',
)



MAX_PAGE_SIZE = 100

app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)
mysql_password = os.environ.get('sql')
app.register_blueprint(swaggerui_blueprint)

@app.route("/movies/<int:movie_id>")
@auth.required


def movie(movie_id):
    
    def remove_nulls(obj):
        return{k:v for k,v in obj.items() if v is not None}
   
    db_conn= pymysql.connect(host="localhost", user="root", password=mysql_password, database="bechdel", cursorclass=pymysql.cursors.DictCursor)
    
    
    with db_conn.cursor() as cursor:
            cursor.execute("""select M.movieId, M.originalTitle as Title, M.primaryTitle as englishTitle, B.rating, M.startYear as year, M.runtimeMinutes from Movies as M 
                           join Bechdel as B 
                           on B.movieId=M.movieId 
                           where M.movieId=%s""", (movie_id, ))
            movie = cursor.fetchone()

            if not movie:
                abort(404)
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
    include_details = bool(int(request.args.get('include_details', 1)))
   
    
    
    db_conn = pymysql.connect(host="localhost", user="root", database="bechdel", password=mysql_password,cursorclass=pymysql.cursors.DictCursor)
    
    
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

            
            template = '%s,'*len(movie_ids)
            template = template[:-1]
            template
            
            genres_by_movie = defaultdict(list)
           
            
            
            with db_conn.cursor() as cursor:
                cursor.execute(f"SELECT movieId, genre FROM MoviesGenres WHERE movieId in ({template})", movie_ids)
                genres = cursor.fetchall()
                for g in genres:
                    genres_by_movie[g['movieId']].append(g['genre'])
        
            people_by_movie = defaultdict(list)

       
            with db_conn.cursor() as cursor:
                cursor.execute(f"""Select movieId, p.personId, p.primaryName as name, p.birthYear, p.deathYear, mp.job, mp.category from MoviesPeople as mp
                             join People as p on p.personID =mp.personID where movieId in ({template})""", movie_ids)
                people=cursor.fetchall()
                for p in people:
                    people_by_movie[p['movieId']].append(remove_nulls(p))
            
            for movie in movies:
                movie_id = movie['movieId']
                movie['genres'] = genres_by_movie[movie_id]
                movie['people'] = people_by_movie[movie_id]

        with db_conn.cursor() as cursor:
            cursor.execute("Select count(*) as total from Movies")
            total = cursor.fetchone()
            last_page=math.ceil(total['total']/ page_size)

         
    db_conn.close()
    return {
        'movies': movies,
        'next_page': f'/movies?page={page+1}&page_size={page_size}&include_details={int(include_details)}',
        'last_page': f'/movies?page={last_page}&page_size={page_size}&include_details={int(include_details)}',
    }        

@app.route("/people/<int:person_id>")
@auth.required

def person(person_id):

  
    db_conn= pymysql.connect(host="localhost", user="root", password=mysql_password, database="bechdel", cursorclass=pymysql.cursors.DictCursor)
    
    
    with db_conn.cursor() as cursor:
            cursor.execute("""select primaryName as Name, BirthYear, deathYear from People as p 
                           where p.personId=%s""", (person_id, ))
            person = cursor.fetchone()

            if not person:
                abort(404)
            person = remove_nulls(person)

    with db_conn.cursor() as cursor:
        cursor.execute("""select p.personId, m.movieId, m.primaryTitle from People as p
                            join MoviesPeople as mp on p.personId = mp.personId
                            join Movies as m on m.movieId = mp.movieId where p.personId=%s""", (person_id, ))
        movies=cursor.fetchall()
    person['movies']=[m['primaryTitle']for m in movies]

    db_conn.close()
    return person

@app.route("/people")
@auth.required
def people():
    
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details', 1)))
    
    
    db_conn = pymysql.connect(host="localhost", user="root", database="bechdel", password=mysql_password,cursorclass=pymysql.cursors.DictCursor)
    
    
    with db_conn.cursor() as cursor:
        cursor.execute("""select p.personId, p.primaryName, p.birthYear, p.deathYear from People as p
                       
            LIMIT %s
            OFFSET %s """, (page_size, (page-1)*page_size))
        people = cursor.fetchall()

         # list of movie IDs
        people_ids = [p['personId'] for p in people]
        
        if include_details:

        #  genres for each movie
            
            template = '%s,'*len(people_ids)
            template = template[:-1]
            template
            
            movies_by_actor = defaultdict(list)
            
            with db_conn.cursor() as cursor:
                cursor.execute(f"""select p.personId, m.movieId, m.primaryTitle from People as p
                            join MoviesPeople as mp on p.personId = mp.personId
                            join Movies as m on m.movieId = mp.movieId where p.personId in ({template})""", people_ids)
                movies= cursor.fetchall()
                movies_by_actor = defaultdict(list)
                
                for m in movies:
                    personId= m['personId']
                    del m['personId']
                    movies_by_actor[personId].append(m)
            
            
            for person in people:
                person_id = person['personId']
                person['movies'] = movies_by_actor[person_id]

        with db_conn.cursor() as cursor:
            cursor.execute("Select count(*) as total from People")
            total = cursor.fetchone()
            last_page=math.ceil(total['total']/ page_size)

         
    db_conn.close()
    return {
        'people': people,
        'next_page': f'/people?page={page+1}&page_size={page_size}&include_details={int(include_details)}',
        'last_page': f'/people?page={last_page}&page_size={page_size}&include_details={int(include_details)}',
    }