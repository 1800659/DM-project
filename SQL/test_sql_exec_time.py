import mariadb
import time
import sys

try:
    conn = mariadb.connect(
        user="root",
        password="1611cris",
        host="127.0.0.1",
        port=3306,
        database="transfermarkt"

    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

cur = conn.cursor()

total_time = 0
arr_string=[
    "select c.competition_id ,c.name,count(g.competition_id) as NumberOfGames from competitions c,games g where g.competition_id = c.competition_id group by c.competition_id order by NumberOfGames DESC;", #q1
    "SELECT c.name as Club_name,p.name as Player_name  FROM players p,clubs c  where p.current_club_id = '131' and p.current_club_id = c.club_id;", #q2
    "select p.name from players p where p.player_id not in (select p.player_id  as Player_name  from players p ,appearances a ,games g ,competitions c where p.player_id =a.player_id and a.game_id =g.game_id and g.competition_id =c.competition_id and c.name = 'uefa-champions-league')", #q3
    "select p.player_id ,p.name,count(a.player_id) as TotalGames  from players p,appearances a where p.player_id = a.player_id group by p.player_id order by TotalGames desc",#q4
    "select p.name ,p.position, sum(a.goals) as GoalInCareer from players p , appearances a where p.player_id = a.player_id group by p.player_id;",#q5
    "select p.name,c.name from clubs c , players p,appearances a where c.club_id  = 131 and c.club_id = a.player_club_id and a.player_id= p.player_id group by p.player_id", #q6
    "select p.name,p.position, sum(a.red_cards) as RED_CARDS from players p , appearances a where a.player_id = p.player_id and p.position = 'Defender' group by p.player_id order by RED_CARDS DESC",#q7
    "select c.name , count(p.partecipation_id) as UCL_Games from clubs c,partecipation p ,games g, competitions c2  where c.club_id = p.club_id and p.game_id = g.game_id and g.competition_id = c2.competition_id and c2.name = 'uefa-champions-league' group by c.club_id order by UCL_Games DESC", #q8
    "select p.name, count(a.appearance_id) as UFC_appearences from players p , appearances a ,games g ,competitions c where  p.last_name = 'Messi' and p.player_id = a.player_id  and a.game_id = g.game_id and g.competition_id = c.competition_id and c.name = 'uefa-champions-league'",#q9
    "select p.name, c2.name, count(a.appearance_id) as UFC_appearences from players p , appearances a ,games g ,competitions c , clubs c2 where p.player_id = a.player_id  and a.game_id = g.game_id  and g.competition_id = c.competition_id and c.name = 'uefa-champions-league' and a.player_club_id = c2.club_id group by p.player_id,c2.club_id order by p.name ASC ,UFC_appearences desc", #q10
    "SELECT p.name AS player_name, p.date_of_birth AS dateOfBirth,SUM(CAST(a.minutes_played AS INT)) AS Minutes_played,SUM(CAST(a.goals AS INT)) AS Goal_scored FROM players p JOIN appearances a ON p.player_id = a.player_id JOIN games g ON a.game_id = g.game_id WHERE DATEDIFF(g.date,p.date_of_birth) <= 8035 AND p.position <> 'Goalkeeper' GROUP BY p.name, p.date_of_birth ORDER BY Goal_scored desc LIMIT 10;", #q11
    "select g.game_id  as Game_ID, c2.name , c3.name, Concat(p.own_goals,'-', p.opponent_goals) as Score,g.`date` from competitions c , games g ,partecipation p ,clubs c2, clubs c3 where c.name = 'serie-a' and c.competition_id = g.competition_id and g.game_id = p.game_id and p.hosting = 'Home' and c2.club_id = p.club_id and p.opponent_id = c3.club_id order by g.`date` ASC", #q12
    "select Player_name, GoalInComp,Competition_Name from(select p.name as Player_name, sum(a.goals) as GoalInComp, c.name as Competition_Name, row_number() over (partition by c.competition_id order by GoalInComp DESC) as Test from players p, appearances a ,games g ,competitions c  where p.player_id = a.player_id and a.game_id = g.game_id and g.competition_id = c.competition_id and (c.name = 'laliga' or c.name = 'serie-a' or c.name='premier-league' or c.name='ligue-1' or c.name='bundesliga') group by  c.competition_id, p.player_id) t where Test = 1", #q13
    "select Player_name, Best_Striker,Club_name from(select p.name as Player_name, sum(a.goals) as Best_Striker, c2.name as Club_name, row_number() over (partition by c2.club_id  order by Best_Striker DESC) as Test from players p, appearances a ,games g ,clubs c2  where p.player_id = a.player_id and a.game_id = g.game_id and a.player_club_id = c2.club_id group by  c2.club_id , p.player_id) t where Test = 1", #q14
    "select c.name as ClubName, sum(p.own_goals) as GoalsScored from clubs c, partecipation p where c.club_id = p.club_id and p.hosting = 'Away' group by c.club_id order by GoalsScored desc limit 10", #q15
    "select p.name as PlayerName, c.name, Count(distinct g.season) as Season from players p ,appearances a , clubs c ,games g where p.player_id = a.player_id and a.player_club_id =c.club_id and a.game_id = g.game_id group by p.player_id,c.club_id having Season >= 5 order by Season desc, PlayerName DESC limit 10",#q16
    "select c2.name as ClubName, c.name as Competition_name, count(g.game_id) as Clean_sheets from competitions c, games g, clubs c2 ,partecipation p where c2.club_id = p.club_id and p.game_id = g.game_id and g.competition_id = c.competition_id and p.opponent_goals ='0' group by c2.club_id,c.competition_id order by Clean_sheets desc limit 10",#q17
    "select p1.name , p2.name, count(distinct g.game_id) as SharedGames from players p1,players p2,  appearances a1 , appearances a2, games g where p1.player_id = a1.player_id and p2.player_id = a2.player_id and g.game_id = a1.game_id and g.game_id = a2.game_id and p1.player_id <> p2.player_id and p1.player_id  < p2.player_id group by p1.player_id,p2.player_id order by SharedGames desc limit 10", #q18
    "select p1.name , p2.name, count(distinct g.game_id) as SharedGames from players p1,players p2,  appearances a1 , appearances a2, games g where p1.player_id = a1.player_id and p2.player_id = a2.player_id and g.game_id = a1.game_id and g.game_id = a2.game_id and p1.player_id <> p2.player_id and p1.player_id  < p2.player_id and a1.player_club_id <> a2.player_club_id  group by p1.player_id,p2.player_id order by SharedGames desc limit 10",#q19
    "select p.name as Player_name, year(current_date())-year(p.date_of_birth) as Age,p.`position` from players p where p.last_season='2022' and year(current_date())-year(p.date_of_birth) >= 35 order by Age desc,p.name desc" #q20
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

# Create an HTML file and write the HTML content
with open("sql_query_results.html", "w") as html_file:
    html_file.write("<html>")
    html_file.write("<head><title>MariaDB Query Results</title></head>")
    html_file.write("<body>")

    html_file.write("<h1>MariaDB Query Results</h1>")


    counter = 0
    for query in arr_string:
        start = time.time()
        cur.execute(query)
        conn.commit()
        end = time.time()
        execution_time = end - start
        result = list(cur)
        num_records = len(result)  # Count the number of records
        counter += 1
        
        html_file.write(f"<h2>Query {counter}: [Execution time: {execution_time:.6f} seconds]</h2>")
        html_file.write(f"<h2>{captions[counter-1]}</h2>")
        html_file.write(f"<p>Number of Records: {num_records}</p>")  # Display the number of records

        html_file.write("<button onclick=\"toggleQueryResults(" + str(counter) + ")\">Toggle Results</button>")
        html_file.write(f"<div id=\"queryResults{counter}\" style=\"display: none;\">")
        html_file.write("<table border='1'>")
        
        if result:
            header = [desc[0] for desc in cur.description]
            html_file.write("<tr>")
            for column in header:
                html_file.write(f"<th>{column}</th>")
            html_file.write("</tr>")
            
            for row in result:
                html_file.write("<tr>")
                for value in row:
                    html_file.write(f"<td>{value}</td>")
                html_file.write("</tr>")
        
        html_file.write("</table>")
        html_file.write("</div>")

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
    
# Close the database connection
conn.close()