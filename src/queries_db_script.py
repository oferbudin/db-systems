from create_db_script import db, HOST, USER, PASSWORD, DATABASE, PORT
import mysql.connector

def example_all_movies(cursor):
    cursor.execute("SELECT * FROM movies")
    result = cursor.fetchall()
    for row in result:
        print(row)

def query_1(cursor):
    # This query calculates the average revenue earned by each actor from the movies they've been in.
    cursor.execute("""
                    SELECT a.name, AVG(r.revenue) AS average_revenue
                    FROM actors a
                    JOIN actor_movies am ON a.id = am.actor_id
                    JOIN revenue r ON am.movie_id = r.movie_id
                    WHERE r.revenue IS NOT NULL
                    GROUP BY a.name
                    ORDER BY average_revenue DESC;
                   """)
    result = cursor.fetchall()
    for row in result:
        print(row)

def query_2(cursor, genre):
    # This query finds the average movie runtime that corresponds to the highest average vote for a specific genre.
    cursor.execute("""
                    SELECT AVG(m.runtime) AS ideal_runtime
                    FROM movies m
                    JOIN ratings r ON m.id = r.movie_id
                    JOIN genres g ON m.genre = g.id
                    WHERE g.name = %s
                    GROUP BY g.id
                    ORDER BY AVG(r.vote_average) DESC
                    LIMIT 1;
                   """, genre)
    result = cursor.fetchall()
    for row in result:
        print(row)

def query_3(cursor):
    # This query returns the director whose film got the highest revenue whilst having a budget lower than average.
    cursor.execute("""
                    SELECT DISTINCT cm.name AS director, m.title, r.revenue, r.budget
                    FROM movies m
                    JOIN crew_members_movies cmm ON m.id = cmm.movie_id
                    JOIN crew_members cm ON cmm.crew_id = cm.id
                    JOIN revenues r ON m.id = r.movie_id
                    WHERE r.budget < (SELECT AVG(budget) FROM revenues) and cm.job = 'director'    
                    order by r.revenue desc
                   """)
    result = cursor.fetchall()
    for row in result:
        print(row)
    

if __name__ == "__main__":
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        port=PORT
    )
    cursor = mydb.cursor()
    
    example_all_movies(cursor)