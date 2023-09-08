import mariadb
import time
import sys

try:
    conn = mariadb.connect(
        user="root",
        password="password",
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
    "SELECT * FROM games WHERE competition_id ='IT1';", #q3
    "select p.player_id ,p.name,count(a.player_id) as TotalGames  from players p,appearances a where p.player_id = a.player_id group by p.player_id order by TotalGames desc",#q4
    "select p.name ,p.position, sum(a.goals) as GoalInCareer from players p , appearances a where p.player_id = a.player_id group by p.player_id;",#q5
    "select p.name ,p.position, sum(a.goals) as GoalInCareer from players p , appearances a where p.player_id = a.player_id group by p.player_id;", #q6
    "select p.name,c.name from clubs c , players p,appearances a where c.club_id  = 131 and c.club_id = a.player_club_id and a.player_id= p.player_id group by p.player_id", #q7
    "select p.name,p.position, count(a.red_cards) as RED_CARDS   from players p , appearances a where a.player_id = p.player_id and p.position = 'Defender' group by p.player_id",#q8
    "select c.name , count(p.partecipation_id) as UCL_Games from clubs c,partecipation p ,games g, competitions c2  where c.club_id = p.club_id and p.game_id = g.game_id and g.competition_id = c2.competition_id and c2.name = 'uefa-champions-league' group by c.club_id", #q9
    "select p.name, count(a.appearance_id) as UFC_appearences from players p , appearances a ,games g ,competitions c where  p.last_name = 'Messi' and p.player_id = a.player_id  and a.game_id = g.game_id and g.competition_id = c.competition_id and c.name = 'uefa-champions-league'",#q10
    "select p.name, c2.name, count(a.appearance_id) as UFC_appearences from players p , appearances a ,games g ,competitions c , clubs c2 where p.player_id = a.player_id  and a.game_id = g.game_id  and g.competition_id = c.competition_id and c.name = 'uefa-champions-league' and a.player_club_id = c2.club_id group by p.player_id,c2.club_id order by p.name ASC ,UFC_appearences desc", #q11
    "SELECT p.name AS player_name, p.date_of_birth AS dateOfBirth,SUM(CAST(a.minutes_played AS INT)) AS Minutes_played,SUM(CAST(a.goals AS INT)) AS Goal_scored FROM players p JOIN appearances a ON p.player_id = a.player_id JOIN games g ON a.game_id = g.game_id WHERE DATEDIFF(g.date,p.date_of_birth) <= 8035 AND p.position <> 'Goalkeeper' GROUP BY p.name, p.date_of_birth ORDER BY Goal_scored desc LIMIT 10;", #q12
    "select Player_name, GoalInComp,Competition_Name from(select p.name as Player_name, sum(a.goals) as GoalInComp, c.name as Competition_Name, row_number() over (partition by c.competition_id order by GoalInComp DESC) as Test from players p, appearances a ,games g ,competitions c  where p.player_id = a.player_id and a.game_id = g.game_id and g.competition_id = c.competition_id and (c.name = 'laliga' or c.name = 'serie-a' or c.name='premier-league' or c.name='ligue-1' or c.name='bundesliga') group by  c.competition_id, p.player_id) t where Test = 1", #q13
    "select Player_name, Best_Striker,Club_name from(select p.name as Player_name, sum(a.goals) as Best_Striker, c2.name as Club_name, row_number() over (partition by c2.club_id  order by Best_Striker DESC) as Test from players p, appearances a ,games g ,clubs c2  where p.player_id = a.player_id and a.game_id = g.game_id and a.player_club_id = c2.club_id group by  c2.club_id , p.player_id) t where Test = 1"

]

# Create an HTML file and write the HTML content
with open("sql_query_results.html", "w") as html_file:
    html_file.write("<html>")
    html_file.write("<head><title>MariaDB Query Results</title></head>")
    html_file.write("<body>")

    counter = 0
    for query in arr_string:
        start = time.time()
        cur.execute(query)
        conn.commit()
        end = time.time()
        execution_time = end - start
        result = list(cur)
        counter += 1

        html_file.write(f"<h2>Query {counter}:  Execution time: {execution_time:.6f} seconds</h2>")

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