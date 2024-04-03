from flask import Flask
import pymysql
import getpass
from flask import request 
import math
from collections import defaultdict
from flask_basicauth import BasicAuth
import json
import cryptography
from flask import abort
from flask_swagger_ui import get_swaggerui_blueprint
import os

mysql_password = os.environ.get('sql')
swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi_football.yaml',
)

MAX_PAGE_SIZE = 100

app = Flask(__name__)

app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)
app.register_blueprint(swaggerui_blueprint)

def remove_nulls(obj):
      return{k:v for k,v in obj.items() if v is not None}

@app.route("/clubs/<int:club_id>")
@auth.required

def club(club_id):

    db_conn= pymysql.connect(host="localhost", user="root", password=mysql_password, database="football", cursorclass=pymysql.cursors.DictCursor)
    
    
    with db_conn.cursor() as cursor:
            cursor.execute("""select c.club_id, c.name, c.domestic_competition_id as competition, cm.name as league, c.stadium_name, sum(p.market_value_in_eur) as total_club_value from clubs as c
            join competitions as cm on c.domestic_competition_id = cm.competition_id
            join players as p on c.club_id = p.current_club_id
            where c.club_id=%s
            GROUP BY 
                    c.club_id, 
                    c.name, 
                    c.domestic_competition_id, 
                    cm.name, 
                    c.stadium_name
          """, (club_id, ))
            club = cursor.fetchone()

            if not club:
                abort(404)
            club = remove_nulls(club)

    with db_conn.cursor() as cursor:
          cursor.execute("""select player_id, name, position, foot, market_value_in_eur from players where current_club_id=%s""", (club_id, ))
          squad=cursor.fetchall()
    club['squad'] = [{'player_id': p['player_id'], 'name': p['name'], 'position': p['position'], 'foot': p['foot'], 'market_value_in_eur': p['market_value_in_eur']} for p in squad]
    squad = [remove_nulls(p) for p in squad]
    club['squad'] = squad
    db_conn.close()
    return club


@app.route("/clubs")
@auth.required
def clubs():
    
    
    
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details', 1)))
    league = request.args.get('league')
  
    
 
    db_conn = pymysql.connect(host="localhost", user="root", database="football", password=mysql_password, cursorclass=pymysql.cursors.DictCursor)
    
    
    with db_conn.cursor() as cursor:
        sql_query = """SELECT c.club_id, c.name, c.domestic_competition_id as competition, cm.name as league, c.stadium_name, sum(p.market_value_in_eur) as total_club_value 
                       FROM clubs as c
                       JOIN competitions as cm on c.domestic_competition_id = cm.competition_id
                       JOIN players as p on c.club_id = p.current_club_id
                       GROUP BY c.club_id, c.name, c.domestic_competition_id, cm.name, c.stadium_name"""
        
        sql_params = []
        
        # If league parameter is provided, filter by league
        if league:
            sql_query += " HAVING c.domestic_competition_id = %s"
            sql_params.append(league)

        sql_query += " LIMIT %s OFFSET %s"
        sql_params.extend([page_size, (page - 1) * page_size])

        cursor.execute(sql_query, sql_params)
        clubs = cursor.fetchall()

         # list of club IDs
        clubs_ids = [c['club_id'] for c in clubs]
        

    if include_details:
            
            template = '%s,'*len(clubs_ids)
            template = template[:-1]
            template
            
            players_by_club = defaultdict(list)
            
    with db_conn.cursor() as cursor:
        cursor.execute(f"select player_id, name, current_club_id as club_id, position, foot, market_value_in_eur from players where current_club_id in ({template})", clubs_ids)
        players = cursor.fetchall()
        for p in players:
            player_info= {
                'player_id': p['player_id'],
                'name': p['name'],
                'position': p['position'],
                'foot': p['foot'],
                'player_value(eur)': p['market_value_in_eur']}
            player_info = remove_nulls(player_info)
            players_by_club[p['club_id']].append(player_info)        
                
    for club  in clubs:
        club_id = club['club_id']
        club['squad'] = players_by_club[club_id]
                

    with db_conn.cursor() as cursor:
            cursor.execute("Select count(*) as total from clubs")
            total = cursor.fetchone()
            last_page=math.ceil(total['total']/ page_size)

         
    db_conn.close()
    return {
        'clubs': clubs,
        'next_page': f'/clubs?page={page+1}&page_size={page_size}&include_details={int(include_details)}',
        'last_page': f'/clubs?page={last_page}&page_size={page_size}&include_details={int(include_details)}',
    }        
