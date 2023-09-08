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
    "MATCH (c:Club {ID: '131'})<-[:MEMBER_OF]-(p:Player) RETURN c.name AS Club_name, p.name AS Player_name",
    "MATCH (c:Competition {ID: 'IT1'})<-[r:PART_OF]-(g:Game) RETURN g.ID, c.name",
    "MATCH (p:Player)<-[r:COLLECT]-(a:Appearance) RETURN p.ID as Player_id, p.name AS Player_name, COUNT(r) AS TotalGames order by TotalGames DESC",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance) RETURN p.ID AS Player_ID, p.name AS player_name, p.position AS player_position,SUM(TOINTEGER(a.goals)) AS GoalInCareer",
    "MATCH (c:Club)<-[:PLAYED_WITH]-(a:Appearance {player_club_id: '131'})-[:COLLECT]->(p:Player) RETURN distinct p.name AS player_name, c.name AS club_name ",
    "MATCH (p:Player {position: 'Defender'})<-[:COLLECT]-(a:Appearance) RETURN p.ID as Player_ID, p.name AS player_name, p.position AS player_position, COUNT(a.red_cards) AS RED_CARDS",
    "MATCH (c:Club)<-[:PARTECIPATE_IN]-(p:Partecipation)-[:REFERS_TO]->(g:Game)-[:PART_OF]->(c2:Competition {name: 'uefa-champions-league'}) RETURN c.ID, c.name AS club_name, COUNT(p.ID) AS UCL_Games order by UCL_Games DESC ",   
    "MATCH (p:Player {last_name: 'Messi'})<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game)-[:PART_OF]->(c:Competition {name: 'uefa-champions-league'}) RETURN p.name AS Player_name, COUNT(a.ID) AS UFC_appearances",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game)-[:PART_OF]->(c:Competition {name: 'uefa-champions-league'}) MATCH (a)-[:PLAYED_WITH]->(c2:Club) RETURN p.name AS player_name, c2.name AS club_name, COUNT(a.ID) AS UFC_appearances ORDER BY p.name ASC, UFC_appearances DESC;",
    "MATCH (p:Player)<-[:COLLECT]-(a:Appearance)-[:APPEREAD_IN]->(g:Game) WITH p, a, g,  a.minutes_played as minutesPlayed,  datetime(p.date_of_birth) as dateOfBirth,  datetime(g.date) as gameDate WITH p, a, g, minutesPlayed, dateOfBirth, gameDate,  duration.between(dateOfBirth, gameDate).years AS ageAtAppearance WHERE ageAtAppearance <=21 AND p.position <> 'Goalkeeper' RETURN p.name AS player_name,(p.date_of_birth) as dateOfBirth, SUM(TOINTEGER(minutesPlayed)) as Minutes_played, SUM(TOINTEGER(a.goals)) as Goal_scored Order by Goal_scored DESC LIMIT 10; ",
    "MATCH (c:Competition {ID: 'IT1'})<-[r:PART_OF]-(g:Game) MATCH (homePartecipation:Partecipation {game_id: g.ID, hosting: 'Home'})-[:PARTECIPATE_IN]->(homeClub:Club) MATCH (awayPartecipation:Partecipation {game_id: g.ID, hosting: 'Away'})-[:PARTECIPATE_IN]->(awayClub:Club) RETURN g.ID AS Game_ID, g.date AS Date, homeClub.name AS Home_Club, awayClub.name AS Away_Club, homePartecipation.own_goals +'-'+ awayPartecipation.own_goals AS Score ORDER BY g.date;",
    "MATCH (competition:Competition) MATCH (player:Player)<-[:COLLECT]-(appearance:Appearance)-[:APPEREAD_IN]->(game:Game)-[:PART_OF]->(competition) WHERE competition.name IN ['serie-a', 'bundesliga', 'laliga', 'ligue-1', 'premier-league'] WITH competition, player, SUM(TOINTEGER(appearance.goals)) AS GoalsScored ORDER BY competition.name, GoalsScored DESC WITH competition, COLLECT({name: player.name, GoalsScored: GoalsScored})[0] AS BestStriker RETURN competition.name AS Competition_name, BestStriker.name AS Best_striker, BestStriker.GoalsScored AS Goal_scored",
    "MATCH (club:Club) MATCH (player:Player)<-[:COLLECT]-(appearance:Appearance)-[:APPEREAD_IN]->(:Game) WHERE appearance.player_club_id = club.ID WITH club, player, SUM(TOINTEGER(appearance.goals)) AS GoalsScored ORDER BY GoalsScored DESC WITH club, COLLECT({name: player.name, GoalsScored: GoalsScored})[0] AS BestStriker RETURN club.name AS Club_name, BestStriker.name AS Best_striker, BestStriker.GoalsScored as Goal_scored"
]

captions = [
    "For each competition, provide the total number of matches.",
    "Retrieve all players from a specific club (FC Barcelona).",
    "Return all matches of a specific competition (Serie A) along with clubs, date, and result.",
    "Count the number of appearances in the history of the database for each player.",
    "For each player, calculate the total number of goals scored in their career, along with their position.",
    "Given a certain club ID, return all the players who have played for that club.",
    "For every defender, report the number of red cards in their career.",
    "Return the number of games played by each club in the UEFA Champions League.",
    "Return the number of appearances by Messi in the UEFA Champions League.",
    "For each player, return the number of appearances in the UEFA Champions League and the club with which they played in those matches.",
    "Return the top 10 players who scored the most goals when they were under 21 years old.",
    "Return all games of a specific competition (Serie A) with the game ID, home and away clubs, score, and date.",
    "Return the top scorer for the top 5 competitions (Serie A, Bundesliga, La Liga, Ligue 1).",
    "Return the best striker for each club."
]



# Driver
driver = GraphDatabase.driver(uri, auth=(username, password), database=db_name)

# Function to execute query and measure execution time
def execute_query(query):
    with driver.session() as session:
        start_time = time.time()
        result = session.run(query)
        end_time = time.time()
        execution_time = end_time - start_time

        # Fetch and store the results in a list
        results_list = list(result)
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
