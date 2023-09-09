from neo4j import GraphDatabase
import time

# Neo4j connection settings
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"
db_name = "transfermarkt"

# Cypher queries
queries = [
    "MATCH (c:Competition)<-[:PART_OF]-(g:Game) RETURN c.ID AS Competition_id, c.name AS Competition_name, COUNT(g) AS NumberOfGames ORDER BY NumberOfGames DESC",
    "MATCH (c:Club {ID: '131'})<-[:MEMBER_OF]-(p:Player) RETURN c.name AS Club_name, p.name AS Player_name Order by Player_name ASC",
    "MATCH (p:Player) WHERE NOT EXISTS {   MATCH (p)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game)-[:PART_OF]->(:Competition {name: 'uefa-champions-league'}) } RETURN p.name AS PlayerName",
    "MATCH (p:Player)<-[r:COLLECT]-(a:Appearance) RETURN p.ID as Player_id, p.name AS Player_name, COUNT(r) AS TotalGames order by TotalGames DESC",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance) RETURN p.ID AS Player_ID, p.name AS player_name, p.position AS player_position,SUM(TOINTEGER(a.goals)) AS GoalInCareer order by GoalInCareer DESC",
    "MATCH (c:Club)<-[:PLAYED_WITH]-(a:Appearance {player_club_id: '131'})-[:COLLECT]->(p:Player) RETURN distinct p.name AS player_name, c.name AS club_name order by player_name ASC ",
    "MATCH (p:Player {position: 'Defender'})<-[:COLLECT]-(a:Appearance) RETURN p.ID AS Player_ID, p.name AS player_name, p.position AS player_position, SUM(TOINTEGER(a.red_cards)) AS RED_CARDS order by RED_CARDS DESC, player_name ASC",
    "MATCH (c:Club)<-[:PARTECIPATE_IN]-(p:Partecipation)-[:REFERS_TO]->(g:Game)-[:PART_OF]->(c2:Competition {name: 'uefa-champions-league'}) RETURN c.ID, c.name AS club_name, COUNT(p.ID) AS UCL_Games order by UCL_Games DESC ",   
    "MATCH (p:Player {last_name: 'Messi'})<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game)-[:PART_OF]->(c:Competition {name: 'uefa-champions-league'}) RETURN p.name AS Player_name, COUNT(a.ID) AS UFC_appearances",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game)-[:PART_OF]->(c:Competition {name: 'uefa-champions-league'}) MATCH (a)-[:PLAYED_WITH]->(c2:Club) RETURN p.ID as player_id, p.name AS player_name, c2.name AS club_name, COUNT(a.ID) AS UFC_appearances ORDER BY p.name ASC, UFC_appearances DESC;",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game) WITH p, a, g,  a.minutes_played as minutesPlayed,  datetime(p.date_of_birth) as dateOfBirth,  datetime(g.date) as gameDate WITH p, a, g, minutesPlayed, dateOfBirth, gameDate,  duration.between(dateOfBirth, gameDate).years AS ageAtAppearance WHERE ageAtAppearance <=21 AND p.position <> 'Goalkeeper' RETURN p.name AS player_name,(p.date_of_birth) as dateOfBirth, SUM(TOINTEGER(minutesPlayed)) as Minutes_played, SUM(TOINTEGER(a.goals)) as Goal_scored Order by Goal_scored DESC, Minutes_played ASC LIMIT 10; ",
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

captions = [
    "Retrieve the total number of matches for each competition and rename the column as 'NumberOfGames'.",
    "Retrieve all players from a specific club, FC Barcelona.",
    "Retrieve all players who have never played in the UEFA Champions League.",
    "Count the number of appearances in the history of the database for each player.",
    "Compute the total number of goals scored by each player in their career and indicate their playing position.",
    "Given a certain club ID, return all the players who have played for that club.",
    "For every defender, return the total number of red cards in their career history.",
    "Return the number of games played by each club in the UEFA Champions League.",
    "Return the number of appearances made by Lionel Messi in the UEFA Champions League.",
    "For each player, return the number of appearances in the UEFA Champions League along with the club they played for in those matches, ordered by player name and appearances.",
    "Return the top 10 players who scored the most goals when they were under 21 years old.",
    "Return details of all games in a specific competition (Serie A) including game ID, home and away clubs, score, and date.",
    "Return the top scorer for the top 5 competitions.",
    "Return the best striker for each club.",
    "Find clubs with the most goals scored in away matches.",
    "Find the 10 most loyal players to clubs (players who have played the most seasons with the same club compared to others).",
    "Find the best defenses for each competition.",
    "Find pairs of different players who have played together most frequently in the same games.",
    "Find pairs of different players who have played against each other most frequently.",
    "Find the oldest active players in each playing position."
]



# Driver
driver = GraphDatabase.driver(uri, auth=(username, password), database=db_name)

# Function to execute query and measure execution time
def execute_query(query):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            start_time = time.time()
            result = tx.run(query)
            # Fetch and store the results in a list
            results_list = list(result)
            tx.commit()
            end_time = time.time()
            execution_time = end_time - start_time

            
            num_records = len(results_list)  # Count the number of records

            
            return execution_time, results_list, num_records

# Create an HTML file and write the HTML content
with open("cypher_query_results.html", "w") as html_file:
    html_file.write("<html>")
    html_file.write("<head><title>Neo4j Query Results</title></head>")
    html_file.write("<body>")

    html_file.write("<h1>Neo4j Query Results</h1>")


    # Execute and measure the time for each query
    for i, query in enumerate(queries):
        execution_time, result, num_records = execute_query(query)
        print(f"Query {i+1} completed. Execution time: {execution_time:.6f} seconds")

        # Display the query description
        html_file.write(f"<h2>Query {i+1}: [Execution time: {execution_time:.6f} seconds]</h2>")
        html_file.write(f"<h2>{captions[i]}</h2>")
        html_file.write(f"<p>Number of Records: {num_records}</p>")  # Display the number of records
        
        # Add a button to show/hide this query's results
        html_file.write(f"<button onclick=\"toggleQueryResults({i})\">Toggle Results</button>")
        
        # Create a div to contain the query results and set its initial style to hidden
        html_file.write(f"<div id=\"queryResults{i}\" style=\"display: none;\">")
        
        html_file.write("<table border='1'>")
        
        # Add the header row with column names
        header = result[0].keys()
        html_file.write("<tr>")
        for column in header:
            html_file.write(f"<th>{column}</th>")
        html_file.write("</tr>")
        
        # Add the data rows
        for record in result:
            html_file.write("<tr>")
            for field in record.values():
                html_file.write(f"<td>{field}</td>")
            html_file.write("</tr>")
        
        html_file.write("</table>")
        
        html_file.write("</div>")  # Close the div for this query's results

    # Add JavaScript functions to show/hide query results
    html_file.write("<script>")
    html_file.write("function toggleQueryResults(queryIndex) {")
    html_file.write("  var queryResultsDiv = document.getElementById(`queryResults${queryIndex}`);")
    html_file.write("  if (queryResultsDiv.style.display === 'none') {")
    html_file.write("    queryResultsDiv.style.display = 'block';")
    html_file.write("  } else {")
    html_file.write("    queryResultsDiv.style.display = 'none';")
    html_file.write("  }")
    html_file.write("}")
    html_file.write("</script>")

    html_file.write("</body>")
    html_file.write("</html>")

# Close the Neo4j driver when done
driver.close()
