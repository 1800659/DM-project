from neo4j import GraphDatabase
import csv

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "1611cris"))
db_name = 'transfermarkt'

players_csv = 'C:/Users/cristian/Desktop/Data Management/project/dataset/relational/players.csv'
clubs_csv  = 'C:/Users/cristian/Desktop/Data Management/project/dataset/relational/clubs.csv'
competitions_csv= 'C:/Users/cristian/Desktop/Data Management/project/dataset/relational/competitions.csv'
appearances_csv= 'C:/Users/cristian/Desktop/Data Management/project/dataset/relational/appearances.csv'
games_csv= 'C:/Users/cristian/Desktop/Data Management/project/dataset/relational/games.csv'
partecipation_csv= 'C:/Users/cristian/Desktop/Data Management/project/dataset/relational/partecipation.csv'

def queryBuilder(ch, p, l):
    query = 'CREATE (n:' + l + ' { ' + 'ID: "{}"'.format(p)
    for e in ch[p]:
        if ch[p][e] != '':
            query = query + ', ' + e + ': "{}"'.format(ch[p][e])
    return query + '})'

pl = {} # dictionary of players
cl = {} #dictionary of clubs
cmp= {} #dictionary of competitions
app = {} #dictionary of appearances
gm = {} #dictionary of games
pr = {} #dictionary of partecipation

print('Extracting data from csv files...')

with open(players_csv,encoding= 'UTF-8') as players, open(clubs_csv, encoding='UTF-8') as clubs,open(competitions_csv, encoding='UTF-8') as competitions,open(appearances_csv, encoding='UTF-8') as appearances,open(games_csv, encoding='UTF-8') as games,open(partecipation_csv, encoding='UTF-8') as partecipation :
    reader1 = csv.DictReader(players)

    # Populate the dictionary of players and clubs
    for row in reader1:
        player_id = row['player_id']
        pl[player_id]={}
        pl[player_id]['first_name'] = row['first_name']
        pl[player_id]['last_name']= row['last_name']
        pl[player_id]['name'] = row['name']
        pl[player_id]['last_season'] = row['last_season']
        pl[player_id]['current_club_id']= row['current_club_id']
        pl[player_id]['player_code'] = row['player_code']
        pl[player_id]['country_of_citizenship'] = row['country_of_citizenship']
        pl[player_id]['date_of_birth'] = row['date_of_birth']
        pl[player_id]['position'] = row['position']
        pl[player_id]['market_value_in_eur'] = row['market_value_in_eur']
        pl[player_id]['contract_expiration_date']= row['contract_expiration_date']
        pl[player_id]['agent_name'] = row ['agent_name']


    reader2 = csv.DictReader(clubs)
    for row in reader2:
        club_id = row['club_id']
        cl[club_id]= {}
        cl[club_id]['club_code']= row['club_code']
        cl[club_id]['name']= row['name']
        cl[club_id]['total_market_value']= row['total_market_value']
        cl[club_id]['squad_size'] = row['squad_size']
        cl[club_id]['average_age'] = row['average_age']
        cl[club_id]['foreign_number'] = row['foreigners_number']
        cl[club_id]['foreigners_percentage'] = row['foreigners_percentage']
        cl[club_id]['national_team_players']= row['national_team_players']
        #cl[club_id]['stadium_name']= row['stadium_name']
        cl[club_id]['stadium_seates'] = row['stadium_seats']
        cl[club_id]['coach_name']=row['coach_name']

    # reader3 = csv.DictReader(competitions)
    # for row in reader3:
    #     competition_id = row['competition_id']
    #     cmp[competition_id]={}
    #     cmp[competition_id]['competition_code'] = row['competition_code']
    #     cmp[competition_id]['name'] = row['name']
    #     cmp[competition_id]['type'] = row['type']
    #     cmp[competition_id]['country_id'] = row['country_id']
    #     cmp[competition_id]['confederation'] = row['confederation']

    # reader4 = csv.DictReader(appearances)
    # for row in reader4:
    #     appearance_id = row['appearance_id']
    #     app[appearance_id]={}
    #     app[appearance_id]['game_id'] = row['game_id']
    #     app[appearance_id]['player_id'] = row['player_id']
    #     app[appearance_id]['competition_id'] = row['competition_id']
    #     app[appearance_id]['yellow_cards'] = row['yellow_cards']
    #     app[appearance_id]['red_cards'] = row['red_cards']
    #     app[appearance_id]['goals'] = row['goals']
    #     app[appearance_id]['assists'] = row['assists'] 
    #     app[appearance_id]['minutes_played'] = row['minutes_played']

    # reader5 = csv.DictReader(games)
    # for row in reader5:
    #     games_id = row['game_id']
    #     gm[games_id]={}
    #     gm[games_id]['competition_id']=row['competition_id']
    #     gm[games_id]['season']= row['season']
    #     gm[games_id]['date'] = row['date']
    #     gm[games_id]['attendance'] = row['attendance']
    #     gm[games_id]['referee'] = row['referee']

    #    reader6 = csv.DictReader(partecipation)
    #    for row in reader6:
    #         partecipation_id = row['partecipation_id']
    #         pr[partecipation_id]['game_id'] = row['game_id']
    #         pr[partecipation_id]['club_id'] = row['club_id']
    #         pr[partecipation_id]['own_goals'] = row['own_goals']
    #         pr[partecipation_id]['opponent_id'] = row['opponent_id']
    #         pr[partecipation_id]['opponent_goals'] = row['opponent_goals']
    #         pr[partecipation_id]['hosting'] = row['hosting']
    #         pr[partecipation_id]['is_win'] = row['is_win']
            
            


    print('Data exraction complete\n')

player_counter = 0
clubs_counter = 0
competition_counter = 0
appearances_counter = 0
games_counter = 0
partecipation_counter=0
with driver.session(database=db_name) as session:
    #   print('Creating competitions nodes...')
    #   for c in cmp:
    #       query = queryBuilder(cmp, c, 'Competition')
    #       session.run(query)
    #       competition_counter += 1
    #   print('{} Competition nodes successfully created\n'.format(competition_counter))

    #   print('Creating clubs nodes...')
    #   for c in cl:
    #       query = queryBuilder(cl, c, 'Clubs')
    #       session.run(query)
    #       clubs_counter += 1
    #   print('{} Clubs nodes successfully created\n'.format(clubs_counter))

    #   print('Creating players nodes...')
    #   for p in pl:
    #       query = queryBuilder(pl, p, 'Player')
    #       session.run(query)
    #       player_counter += 1
    #   print('{} Players nodes successfully created\n'.format(player_counter))

    #   print('Creating appearances nodes...')
    #   for a in app:
    #       query = queryBuilder(app, a, 'Appearance')
    #       session.run(query)
    #       appearances_counter+= 1
    #   print('{} Appearances nodes successfully created\n'.format(appearances_counter))

    #   print('Creating games nodes...')
    #   for g in gm:
    #       query = queryBuilder(gm, g, 'Game')
    #       session.run(query)
    #       games_counter+= 1
    #   print('{} Games nodes successfully created\n'.format(games_counter))

        # print('Creating partecipations nodes...')
        # for p in pr:
        #     query = queryBuilder(pr, p, 'Partecipation')
        #     session.run(query)
        #     partecipation_counter+= 1
        # print('{} Games nodes successfully created\n'.format(partecipation_counter))

        

        driver.close()

club_counter =0
with driver.session(database=db_name) as session:
    print('Creating edges connecting player and clubs...')
    for p in pl:
        if pl[p]['current_club_id'] != '' and pl[p]['current_club_id'] in cl:
            rel_query = (
                'MATCH (p:Player), (c:Clubs) WHERE p.player_id="' + p + '" AND c.club_id="' + pl[p]['current_club_id'] + '" CREATE (p)-[:MEMBER_OF]->(c)'
            )
            session.run(rel_query)
            club_counter += 1

    print('{} players-club edges successfully created\n'.format(club_counter))
    driver.close()
     
