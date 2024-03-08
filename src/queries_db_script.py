from create_db_script import db, HOST, USER, PASSWORD, DATABASE, PORT
import mysql.connector


mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        port=PORT
    )
cursor = mydb.cursor()


def query_1():
    """
    This query calculates the average revenue earned by each actor from the movies they've been in.
    Only for movies that have a Second Film Editor in the crew.
    """
    cursor.execute("""
                    SELECT a.name, AVG(m.revenue) AS average_revenue
                    FROM actors a
                    JOIN actor_movies am ON a.id = am.actor_id
                    JOIN movies m ON am.movie_id = m.id
                    WHERE m.revenue IS NOT NULL
                    AND EXISTS (
                        select * from crew_members cm 
                        join crew_members_movies cmm on cm.id = cmm.crew_id and cmm.movie_id = m.id
                        where cm.job = 'Second Film Editor'
                    )
                    GROUP BY a.name
                    ORDER BY average_revenue DESC;
                   """)
    result = cursor.fetchall()
    return result

def query_2(genre):
    """
    This query finds the average movie runtime that corresponds to the highest average vote for a specific genre.
    """
    cursor.execute("""
                    SELECT AVG(m.runtime) AS ideal_runtime
                    FROM movies m
                    JOIN genres g ON m.genre = g.id
                    WHERE g.name = %s
                    GROUP BY g.id
                    ORDER BY AVG(m.vote_average) DESC
                    LIMIT 1;
                   """, (genre,))
    result = cursor.fetchall()
    return result

def query_3():
    """
    This query returns the director whose film got the highest revenue whilst having a budget lower than average.
    """
    cursor.execute("""
                    SELECT DISTINCT cm.name AS director, m.title, m.revenue, m.budget
                    FROM movies m
                    JOIN crew_members_movies cmm ON m.id = cmm.movie_id
                    JOIN crew_members cm ON cmm.crew_id = cm.id
                    WHERE m.budget < (SELECT AVG(budget) FROM movies) and cm.job = 'director'    
                    order by m.revenue desc
                   """)
    result = cursor.fetchall()
    return result
    
def query_4():
    """
    This query returns the actor who has the highest average revenue from the movies they've been in.
    only if they've been in more than one movie.
    """
    cursor.execute("""
                   SELECT a.name AS actor_name, AVG(m.revenue) AS average_revenue
                    FROM actors a
                    JOIN actor_movies am ON a.id = am.actor_id
                    JOIN movies m ON am.movie_id = m.id
                    GROUP BY a.name, m.title
                    HAVING count(m.title) > 1
                    order by AVG(m.revenue) desc
                    limit 1;
                """)
    result = cursor.fetchall()
    return result

def query_5(genre):
    """
    This query returns the actress who has the highest average rating in a specific genre.
    """
    query = """
        SELECT a.name AS actress_name, AVG(m.vote_average) AS average_rating
        FROM actors a
        JOIN actor_movies am ON a.id = am.actor_id
        JOIN movies m ON am.movie_id = m.id
        JOIN genres g ON m.genre = g.id
        WHERE g.name = %s
        GROUP BY a.name
        ORDER BY AVG(m.vote_average) DESC
        LIMIT 1;
    """
    cursor.execute(query, (genre,))
    result = cursor.fetchall()
    return result

def query_6():
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

def query_7(search):
    """
    This query returns the movie id, title and overview of the movies that thier title or overview match the search term.
    """
    query = """
        SELECT id, title, overview
        FROM movies
        WHERE MATCH(title, overview) AGAINST (%s IN NATURAL LANGUAGE MODE);
    """
    cursor.execute(query, (search,))
    result = cursor.fetchall()
    return result

def query_8(actor_name):
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
