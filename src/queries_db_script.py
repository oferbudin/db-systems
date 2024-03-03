from create_db_script import db, HOST, USER, PASSWORD, DATABASE, PORT
import mysql.connector


def query_1(cursor):
    """
    This query calculates the average revenue earned by each actor from the movies they've been in.
    """
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
    return result

def query_2(cursor, genre):
    """
    This query finds the average movie runtime that corresponds to the highest average vote for a specific genre.
    """
    cursor.execute("""
                    SELECT AVG(m.runtime) AS ideal_runtime
                    FROM movies m
                    JOIN ratings r ON m.id = r.movie_id
                    JOIN genres g ON m.genre = g.id
                    WHERE g.name = %s
                    GROUP BY g.id
                    ORDER BY AVG(r.vote_average) DESC
                    LIMIT 1;
                   """, (genre,))
    result = cursor.fetchall()
    return result

def query_3(cursor):
    """
    This query returns the director whose film got the highest revenue whilst having a budget lower than average.
    """
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
    return result
    
def query_4(cursor):
    """
    This query returns the actor who has the highest average revenue from the movies they've been in.
    only if they've been in more than one movie.
    """
    cursor.execute("""
                   SELECT a.name AS actor_name, m.title AS movie_title, AVG(r.revenue) AS average_revenue
                    FROM actors a
                    JOIN actor_movies am ON a.id = am.actor_id
                    JOIN revenues r ON am.movie_id = r.movie_id
                    JOIN movies m ON am.movie_id = m.id
                    GROUP BY a.name, m.title
                    HAVING count(m.title) > 1
                    order by AVG(r.revenue) desc
                """)
    result = cursor.fetchall()
    return result

def query_5(cursor, genre):
    """
    This query returns the actress who has the highest average rating in a specific genre.
    """
    query = """
        SELECT a.name AS actress_name, AVG(r.vote_average) AS average_rating
        FROM actors a
        JOIN actor_movies am ON a.id = am.actor_id
        JOIN movies m ON am.movie_id = m.id
        JOIN genres g ON m.genre = g.id
        JOIN ratings r ON m.id = r.movie_id
        WHERE g.name = %s
        GROUP BY a.name
        ORDER BY AVG(r.vote_average) DESC
        LIMIT 1;
    """
    cursor.execute(query, (genre,))
    result = cursor.fetchall()
    return result

def query_6(cursor):
    """
    This query is a util, it returns the names of all the genres in the database.
    """
    cursor.execute("""
        SELECT name
        FROM genres
    """)
    result = cursor.fetchall()
    for res in result:
        print(res)
    return result

def query_7(curser, search):
    """
    This query returns the movie id, title and overview of the movies that thier title or overview match the search term.
    """
    query = """
        SELECT id, title, overview
        FROM movies
        WHERE MATCH(title, overview) AGAINST (%s IN NATURAL LANGUAGE MODE);
    """
    curser.execute(query, (search,))
    result = cursor.fetchall()
    return result

def query_8(cursor, actor_name):
    """
    This query returns the actor's name, the movie title that actor_name is in the actor name
    """
    query = """
        SELECT a.id, a.name, m.title
        FROM actors a 
        join actor_movies am on a.id = am.actor_id
        join movies m on am.movie_id = m.id
        WHERE MATCH(name) AGAINST (%s IN NATURAL LANGUAGE MODE);
    """
    cursor.execute(query, (actor_name,))
    result = cursor.fetchall()
    return result

if __name__ == "__main__":
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        port=PORT
    )
    cursor = mydb.cursor()
    