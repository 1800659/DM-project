from neo4j import GraphDatabase
import time

# Neo4j connection settings
uri = "bolt://localhost:7687"
username = "neo4j"
password = "1611cris"
db_name = "transfermarkt"

# Cypher queries
queries = [
    "MATCH (c:Competition)<-[:PART_OF]-(g:Game) RETURN c.ID AS Competition_id, c.name AS Competition_name, COUNT(g) AS NumberOfGames ORDER BY NumberOfGames DESC",
    "MATCH (c:Club {ID: '131'})<-[:MEMBER_OF]-(p:Player) RETURN c.name AS Club_name, p.name AS Player_name",
    "MATCH (p:Player) WHERE NOT EXISTS {   MATCH (p)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game)-[:PART_OF]->(:Competition {name: 'uefa-champions-league'}) } RETURN p.name AS PlayerName",
    "MATCH (p:Player)<-[r:COLLECT]-(a:Appearance) RETURN p.ID as Player_id, p.name AS Player_name, COUNT(r) AS TotalGames order by TotalGames DESC",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance) RETURN p.ID AS Player_ID, p.name AS player_name, p.position AS player_position,SUM(TOINTEGER(a.goals)) AS GoalInCareer",
    "MATCH (c:Club)<-[:PLAYED_WITH]-(a:Appearance {player_club_id: '131'})-[:COLLECT]->(p:Player) RETURN distinct p.name AS player_name, c.name AS club_name ",
    "MATCH (p:Player {position: 'Defender'})<-[:COLLECT]-(a:Appearance) RETURN p.ID AS Player_ID, p.name AS player_name, p.position AS player_position, SUM(TOINTEGER(a.red_cards)) AS RED_CARDS order by RED_CARDS DESC",
    "MATCH (c:Club)<-[:PARTECIPATE_IN]-(p:Partecipation)-[:REFERS_TO]->(g:Game)-[:PART_OF]->(c2:Competition {name: 'uefa-champions-league'}) RETURN c.ID, c.name AS club_name, COUNT(p.ID) AS UCL_Games order by UCL_Games DESC ",   
    "MATCH (p:Player {last_name: 'Messi'})<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game)-[:PART_OF]->(c:Competition {name: 'uefa-champions-league'}) RETURN p.name AS Player_name, COUNT(a.ID) AS UFC_appearances",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game)-[:PART_OF]->(c:Competition {name: 'uefa-champions-league'}) MATCH (a)-[:PLAYED_WITH]->(c2:Club) RETURN p.ID as player_id, p.name AS player_name, c2.name AS club_name, COUNT(a.ID) AS UFC_appearances ORDER BY p.name ASC, UFC_appearances DESC;",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game) WITH p, a, g,  a.minutes_played as minutesPlayed,  datetime(p.date_of_birth) as dateOfBirth,  datetime(g.date) as gameDate WITH p, a, g, minutesPlayed, dateOfBirth, gameDate,  duration.between(dateOfBirth, gameDate).years AS ageAtAppearance WHERE ageAtAppearance <=21 AND p.position <> 'Goalkeeper' RETURN p.name AS player_name,(p.date_of_birth) as dateOfBirth, SUM(TOINTEGER(minutesPlayed)) as Minutes_played, SUM(TOINTEGER(a.goals)) as Goal_scored Order by Goal_scored DESC LIMIT 10; ",
    "MATCH (c:Competition {ID: 'IT1'})<-[r:PART_OF]-(g:Game) MATCH (homePartecipation:Partecipation {game_id: g.ID, hosting: 'Home'})-[:PARTECIPATE_IN]->(homeClub:Club) MATCH (awayPartecipation:Partecipation {game_id: g.ID, hosting: 'Away'})-[:PARTECIPATE_IN]->(awayClub:Club) RETURN g.ID AS Game_ID, g.date AS Date, homeClub.name AS Home_Club, awayClub.name AS Away_Club, homePartecipation.own_goals +'-'+ awayPartecipation.own_goals AS Score ORDER BY g.date;",
    "MATCH (competition:Competition) MATCH (player:Player)<-[:COLLECT]-(appearance:Appearance)-[:APPEREAD_IN]->(game:Game)-[:PART_OF]->(competition) WHERE competition.name IN ['serie-a', 'bundesliga', 'laliga', 'ligue-1', 'premier-league'] WITH competition, player, SUM(TOINTEGER(appearance.goals)) AS GoalsScored ORDER BY competition.name, GoalsScored DESC WITH competition, COLLECT({name: player.name, GoalsScored: GoalsScored})[0] AS BestStriker RETURN competition.name AS Competition_name, BestStriker.name AS Best_striker, BestStriker.GoalsScored AS Goal_scored",
    "MATCH (club:Club) MATCH (player:Player)<-[:COLLECT]-(appearance:Appearance)-[:APPEREAD_IN]->(:Game) WHERE appearance.player_club_id = club.ID WITH club, player, SUM(TOINTEGER(appearance.goals)) AS GoalsScored ORDER BY GoalsScored DESC WITH club, COLLECT({name: player.name, GoalsScored: GoalsScored})[0] AS BestStriker RETURN club.name AS Club_name, BestStriker.name AS Best_striker, BestStriker.GoalsScored as Goal_scored",
    "MATCH (c:Club)<-[:PARTECIPATE_IN]-(p:Partecipation)-[:REFERS_TO]->(g:Game) WHERE p.hosting = 'Away' RETURN c.name AS ClubName, SUM(TOINTEGER(p.own_goals)) AS GoalsScored ORDER BY GoalsScored DESC LIMIT 10",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game) MATCH (club:Club {ID: a.player_club_id}) WITH p, a, g, club, g.season AS Season WITH p, club, COLLECT(DISTINCT Season) AS Seasons WHERE SIZE(Seasons) >= 5 RETURN p.name AS PlayerName, club.name AS ClubName, SIZE(Seasons) AS Seasons ORDER BY Seasons DESC, PlayerName DESC LIMIT 10",
    "MATCH (c:Club)<-[:PARTECIPATE_IN]-(p:Partecipation)-[:REFERS_TO]->(g:Game)-[:PART_OF]->(c1:Competition) WHERE p.opponent_goals = '0' RETURN c.name AS ClubName, c1.name as Competition_name, COUNT(g) AS Clean_sheets ORDER BY Clean_sheets DESC LIMIT 10",
    "MATCH (p1:Player)<-[:COLLECT]-(a1:Appearance)-[:APPEREAD_IN]->(g:Game), (p2:Player)<-[:COLLECT]-(a2:Appearance)-[:APPEREAD_IN]->(g) WHERE p1 <> p2 AND p1 < p2 WITH p1, p2, COUNT(DISTINCT g) AS SharedGames WHERE SharedGames >= 5 RETURN p1.name AS Player1, p2.name AS Player2, SharedGames ORDER BY SharedGames DESC LIMIT 10",
    "MATCH (p1:Player)<-[:COLLECT]-(a1:Appearance)-[:APPEREAD_IN]->(g:Game), (p2:Player)<-[:COLLECT]-(a2:Appearance)-[:APPEREAD_IN]->(g) WHERE p1 <> p2 AND a1.player_club_id <> a2.player_club_id AND p1 < p2 WITH p1, p2, COUNT(DISTINCT g) AS SharedGames RETURN p1.name AS Player1, p2.name AS Player2, SharedGames ORDER BY SharedGames DESC LIMIT 10",
    "MATCH (p:Player) WHERE p.last_season = '2022' WITH p, datetime(p.date_of_birth) AS BirthDate WITH p, BirthDate, datetime().year - BirthDate.year AS Age WHERE Age >= 35 RETURN p.name AS PlayerName, p.position AS Position, Age ORDER BY Age DESC, PlayerName DESC"
]

driver = GraphDatabase.driver(uri, auth=(username, password), database=db_name)

# Function to execute query and measure execution time
def execute_query(query):
    with driver.session() as session:
        total_time = 0
        for i in range(1000):
            with session.begin_transaction() as tx:
                start_time = time.time()
                result = tx.run(query)
                # Fetch and store the results in a list
                results_list = list(result)
                tx.commit()
                total_time += time.time() - start_time
            num_records = len(results_list)  # Count the number of records

            
            return total_time, results_list, num_records
        
for i, query in enumerate(queries):
    execution_time, result, num_records = execute_query(query)
    avg_time = execution_time/1000
    print(f"Query {i+1} completed. average execution time: {execution_time:.6f} seconds")