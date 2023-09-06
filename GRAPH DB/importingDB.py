from neo4j import GraphDatabase
import csv

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))
db_name = 'transfermarkt'

players_csv = '../TRANSFERMARKT FIXED CSV/players.csv'
clubs_csv  = '../TRANSFERMARKT FIXED CSV/clubs.csv'
competitions_csv= '../TRANSFERMARKT FIXED CSV/competitions.csv'
appearances_csv= '../TRANSFERMARKT FIXED CSV/appearances.csv'
games_csv= '../TRANSFERMARKT FIXED CSV/games.csv'
partecipation_csv= '../TRANSFERMARKT FIXED CSV/partecipation.csv'

def queryBuilder(ch, p, l):
    query = 'CREATE (n:' + l + ' { ' + 'ID: "{}"'.format(p)
    for e in ch[p]:
        if ch[p][e] != '':
            query = query + ', ' + e + ': "{}"'.format(ch[p][e])
    return query + '})'


########### DICTIONARY #############

pl = {} # dictionary of players
cl = {} #dictionary of clubs
cmp= {} #dictionary of competitions
app = {} #dictionary of appearances
gm = {} #dictionary of games
pr = {} #dictionary of partecipation

print('Extracting data from csv files...')

with open(players_csv,encoding= 'UTF-8') as players, open(clubs_csv, encoding='UTF-8') as clubs,open(competitions_csv, encoding='UTF-8') as competitions,open(appearances_csv, encoding='UTF-8') as appearances,open(games_csv, encoding='UTF-8') as games,open(partecipation_csv, encoding='UTF-8') as partecipation :
    
    reader1 = csv.DictReader(players)
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
        cl[club_id]['stadium_seates'] = row['stadium_seats']
        cl[club_id]['coach_name']=row['coach_name']

    reader3 = csv.DictReader(competitions)
    for row in reader3:
        competition_id = row['competition_id']
        cmp[competition_id]={}
        cmp[competition_id]['competition_code'] = row['competition_code']
        cmp[competition_id]['name'] = row['name']
        cmp[competition_id]['type'] = row['type']
        cmp[competition_id]['country_id'] = row['country_id']
        cmp[competition_id]['confederation'] = row['confederation']

    reader4 = csv.DictReader(appearances)
    for row in reader4:
        appearance_id = row['appearance_id']
        app[appearance_id]={}
        app[appearance_id]['game_id'] = row['game_id']
        app[appearance_id]['player_id'] = row['player_id']
        app[appearance_id]['player_club_id'] = row['player_club_id']
        app[appearance_id]['yellow_cards'] = row['yellow_cards']
        app[appearance_id]['red_cards'] = row['red_cards']
        app[appearance_id]['goals'] = row['goals']
        app[appearance_id]['assists'] = row['assists'] 
        app[appearance_id]['minutes_played'] = row['minutes_played']

    reader5 = csv.DictReader(games)
    for row in reader5:
        games_id = row['game_id']
        gm[games_id]={}
        gm[games_id]['competition_id']=row['competition_id']
        gm[games_id]['season']= row['season']
        gm[games_id]['date'] = row['date']
        gm[games_id]['attendance'] = row['attendance']
        gm[games_id]['referee'] = row['referee']

    reader6 = csv.DictReader(partecipation)
    for row in reader6:
        partecipation_id = row['partecipation_id']
        pr[partecipation_id] = {}
        pr[partecipation_id]['game_id'] = row['game_id']
        pr[partecipation_id]['club_id'] = row['club_id']
        pr[partecipation_id]['own_goals'] = row['own_goals']
        pr[partecipation_id]['opponent_id'] = row['opponent_id']
        pr[partecipation_id]['opponent_goals'] = row['opponent_goals']
        pr[partecipation_id]['hosting'] = row['hosting']
        pr[partecipation_id]['is_win'] = row['is_win']
            
    print('Data extraction complete\n')

########### NODES ###########

player_counter = 0
clubs_counter = 0
competition_counter = 0
appearances_counter = 0
games_counter = 0
partecipation_counter = 0

with driver.session(database=db_name) as session:
    print('Creating competitions nodes...')
    for c in cmp:
        query = queryBuilder(cmp, c, 'Competition')
        session.run(query)
        competition_counter += 1
    print('{} Competition nodes successfully created\n'.format(competition_counter))

    print('Creating clubs nodes...')
    for c in cl:
        query = queryBuilder(cl, c, 'Club')
        session.run(query)
        clubs_counter += 1
    print('{} Club nodes successfully created\n'.format(clubs_counter))

    print('Creating players nodes...')
    for p in pl:
        query = queryBuilder(pl, p, 'Player')
        session.run(query)
        player_counter += 1
    print('{} Players nodes successfully created\n'.format(player_counter))

    print('Creating appearances nodes...')
    for a in app:
        query = queryBuilder(app, a, 'Appearance')
        session.run(query)
        appearances_counter+= 1
    print('{} Appearance nodes successfully created\n'.format(appearances_counter))

    print('Creating games nodes...')
    for g in gm:
        query = queryBuilder(gm, g, 'Game')
        session.run(query)
        games_counter+= 1
    print('{} Games nodes successfully created\n'.format(games_counter))

    print('Creating partecipations nodes...')
    for p in pr:
        query = queryBuilder(pr, p, 'Partecipation')
        session.run(query)
        partecipation_counter+= 1
    print('{} Games nodes successfully created\n'.format(partecipation_counter))

driver.close()

############ RELATIONSHIPS #############

club_counter = 0
competition_counter = 0
club_counter1 = 0
counter_game = 0
counter_game1 = 0
counter_player = 0
counter_clubs = 0

with driver.session(database=db_name) as session:
    ############## PLAYERS -> CLUB #####################
    print('Creating edges connecting player and clubs...')
    for p in pl:
        if pl[p]['current_club_id'] != '' and pl[p]['current_club_id'] in cl.keys():
            rel_query ='MATCH (p:Player), (c:Club) WHERE p.ID="' + p + '" AND c.ID="' + pl[p]['current_club_id'] + '" CREATE (p)-[:MEMBER_OF]->(c)'
            session.run(rel_query)
            club_counter += 1

    print('{} players-club edges successfully created\n'.format(club_counter))

    ############# GAMES -> COMPETITION #####################
    print('Creating edges connecting games and competitions...')
    for g in gm :
        if gm[g]['competition_id'] != '' and gm[g]['competition_id'] in cmp.keys():
            rel_query ='MATCH (g:Game), (c:Competition) WHERE g.ID="' + g + '" AND c.ID="' + gm[g]['competition_id'] + '" CREATE (g)-[:PART_OF]->(c)'
            session.run(rel_query)
            competition_counter += 1

    print('{} games-competitions edges successfully created\n'.format(competition_counter))

    ############## PARTECIPATION -> CLUB #####################
    print('Creating edges connecting partecipation and clubs...')
    for p in pr :
        if pr[p]['club_id'] != '' and pr[p]['club_id'] in cl.keys():
            rel_query ='MATCH (p:Partecipation), (c:Club) WHERE p.ID="' + p + '" AND c.ID="' + pr[p]['club_id'] + '" CREATE (p)-[:PARTECIPATE_IN]->(c)'
            session.run(rel_query)
            club_counter1 += 1
    print('{} partecipation-clubs edges successfully created\n'.format(club_counter1))

    ############## PARTECIPATION -> GAMES #####################
    print('Creating edges connecting partecipation and games...')
    for p in pr :
        if pr[p]['game_id'] != '' and pr[p]['game_id'] in gm.keys():
            rel_query ='MATCH (p:Partecipation), (g:Game) WHERE p.ID="' + p + '" AND g.ID="' + pr[p]['game_id'] + '" CREATE (p)-[:REFERS_TO]->(g)'
            session.run(rel_query)
            counter_game += 1
    print('{} partecipation-games edges successfully created\n'.format(counter_game))

    ############## APPEARANCES -> GAMES #####################
    print('Creating edges connecting appearances and games...')
    for a in app :
        if app[a]['game_id'] != '' and app[a]['game_id'] in gm.keys():
            rel_query ='MATCH (a:Appearance), (g:Game) WHERE a.ID="' + a + '" AND g.ID="' + app[a]['game_id'] + '" CREATE (a)-[:APPEREAD_IN]->(g)'
            session.run(rel_query)
            counter_game1 += 1
    print('{} appearances-games edges successfully created\n'.format(counter_game1))

    ############## APPEARANCES -> PLAYER #####################
    print('Creating edges connecting appearances and player...')
    for a in app :
        if app[a]['player_id'] != '' and app[a]['player_id'] in pl.keys():
            rel_query ='MATCH (a:Appearance), (p:Player) WHERE a.ID="' + a + '" AND p.ID="' + app[a]['player_id'] + '" CREATE (a)-[:COLLECT]->(p)'
            session.run(rel_query)
            counter_player += 1
    print('{} appearances-players edges successfully created\n'.format(counter_player))

    ############## APPEARANCES -> CLUBS #####################
    print('Creating edges connecting appearances and clubs...')
    for a in app :
        if app[a]['player_club_id'] != '' and app[a]['player_club_id'] in cl.keys():
            rel_query ='MATCH (a:Appearances), (c:Clubs) WHERE a.ID="' + a + '" AND c.ID="' + app[a]['player_club_id'] + '" CREATE (a)-[:PLAYED_WITH]->(c)'
            session.run(rel_query)
            counter_clubs += 1
    print('{} appearances-clubs edges successfully created\n'.format(counter_clubs))

driver.close()
