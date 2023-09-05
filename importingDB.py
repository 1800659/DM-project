from neo4j import GraphDatabase
import csv


uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "1611cris"))
db_name = 'transfermarkt'

players_csv = 'C:/Users/cristian/Desktop/Data Management/project/dataset/relational/players.csv'
clubs_csv    = 'C:/Users/cristian/Desktop/Data Management/project/dataset/relational/clubs.csv'

def queryBuilder(ch, p, l):
    query = 'CREATE (n:' + l + ' { ' + 'ID: "{}"'.format(p)
    for e in ch[p]:
        if ch[p][e] != '':
            query = query + ', ' + e + ': "{}"'.format(ch[p][e])
    return query + '})'

pl = {} # dictionary of players
cl = {} #dictionary of clubs

print('Extracting data from csv files...')

with open(players_csv,encoding = 'UTF-8') as players, open(clubs_csv) as clubs:
    reader1 = csv.DictReader(players)

    # Populate the dictionary of Characters and the list of Houses
    for row in reader1:
        player_id = row['player_id']
        pl[player_id]={}
        pl[player_id]['first_name'] = row['first_name']
        pl[player_id]['last_name']= row['last_name']
        pl[player_id]['name'] = row['name']
        pl[player_id]['last_season'] = row['last_season']
        pl[player_id]['current_club_id']= row['current_club_id']
        pl[player_id]['player_code'] = row['player_id']
        pl[player_id]['country_of_citizenship'] = row['country_of_citizenship']
        pl[player_id]['date_of_birth'] = row['date_of_birth']
        pl[player_id]['position'] = row['position']
        pl[player_id]['market_value_in_eur'] = row['market_value_in_eur']
        pl[player_id]['contract_expiration_date']= row['contract_expiration_date']
        pl[player_id]['agent_name'] = row ['agent_name']

print('Data exraction complete\n')

player_counter = 0

with driver.session(database=db_name) as session:
      print('Creating players nodes...')
      for p in pl:
          query = queryBuilder(pl, p, 'Player:'+ pl[p]['name'])
          session.run(query)
          player_counter += 1
      print('{} characters nodes successfully created\n'.format(player_counter))

driver.close()

