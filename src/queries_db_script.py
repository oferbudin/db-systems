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
    This query calculates the average revenue earned by each actor for the movies they've been in.
    This is only calculated for movies with a second film editor in their crew.
    """
    get_average_revenue_per_actor = """
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
                                    """
    cursor.execute(get_average_revenue_per_actor)
    average_revenue_per_actor = cursor.fetchall()
    return average_revenue_per_actor

def query_2(genre):
    """
    This query finds the average movie runtime that corresponds to the highest average vote for a specific genre.
    """
    get_optimal_runtime_for_genre = """
                                    WITH GenreStats AS (
                                    SELECT g.name AS genre_name,
                                        AVG(m.runtime) AS ideal_runtime,
                                        AVG(m.vote_average) AS avg_vote_avg
                                    FROM movies m
                                    JOIN genres g ON m.genre = g.id
                                    Where g.name = %s
                                    GROUP BY g.name
                                    ) SELECT ideal_runtime
                                    FROM GenreStats g
                                    WHERE avg_vote_avg = (
                                        SELECT MAX(avg_vote_avg) FROM GenreStats
                                    );
                                    """
    cursor.execute(get_optimal_runtime_for_genre, (genre, ))
    optimal_runtime_for_genre = cursor.fetchall()
    return optimal_runtime_for_genre

def query_3():
    """
    This query returns the director whose film got the highest revenue whilst having a budget lower than average.
    """
    get_director_with_best_revenue_budget_ratio = """
                                                SELECT DISTINCT cm.name AS director, m.title, m.revenue, m.budget
                                                FROM movies m
                                                JOIN crew_members_movies cmm ON m.id = cmm.movie_id
                                                JOIN crew_members cm ON cmm.crew_id = cm.id
                                                WHERE m.budget < (SELECT AVG(budget) FROM movies) and cm.job = 'director'    
                                                order by m.revenue desc
                                                """
    cursor.execute(get_director_with_best_revenue_budget_ratio)
    director_with_best_revenue_budget_ratio = cursor.fetchall()
    return director_with_best_revenue_budget_ratio
    
def query_4():
    """
    This query returns the actor who has the highest average revenue from the movies they've been in.
    This calculation is only relevant for actors that have been in more than one movie.
    """
    get_actor_with_highest_revenue = """
                                    SELECT a.name AS actor_name, AVG(m.revenue) AS average_revenue
                                    FROM actors a
                                    JOIN actor_movies am ON a.id = am.actor_id
                                    JOIN movies m ON am.movie_id = m.id
                                    GROUP BY a.name, m.title
                                    HAVING count(m.title) > 1
                                    order by AVG(m.revenue) desc
                                    limit 1;
                                    """
    cursor.execute(get_actor_with_highest_revenue)
    actor_with_highest_revenue = cursor.fetchall()
    return actor_with_highest_revenue

def query_5(genre):
    """
    This query returns the actor who has the highest average rating in a specific genre.
    This is only calculated if the genre has more than 5 movies.
    """
    get_actor_with_highest_rating_per_genre = """
                                            SELECT a.name AS actor_name, AVG(m.vote_average) AS average_rating
                                            FROM actors a
                                            JOIN actor_movies am ON a.id = am.actor_id
                                            JOIN movies m ON am.movie_id = m.id
                                            JOIN genres g ON m.genre = g.id
                                            WHERE g.name = %s
                                            AND (
                                                SELECT COUNT(*)
                                                FROM movies
                                                WHERE genre = g.id
                                            ) > 5
                                            GROUP BY a.name
                                            HAVING AVG(m.vote_average) = (
                                                SELECT MAX(avg_vote_average)
                                                FROM (
                                                    SELECT AVG(m2.vote_average) AS avg_vote_average
                                                    FROM actors a2
                                                    JOIN actor_movies am2 ON a2.id = am2.actor_id
                                                    JOIN movies m2 ON am2.movie_id = m2.id
                                                    JOIN genres g2 ON m2.genre = g2.id
                                                    WHERE g2.name = %s
                                                    AND (
                                                        SELECT COUNT(*)
                                                        FROM movies
                                                        WHERE genre = g2.id
                                                    ) > 5
                                                    GROUP BY a2.name
                                                ) AS max_avg_vote_subquery
                                            );
                                            """
    cursor.execute(get_actor_with_highest_rating_per_genre, (genre, genre))
    actor_with_highest_rating_per_genre = cursor.fetchall()
    return actor_with_highest_rating_per_genre

def query_6():
    """
    This query is a util, it returns the names of all the genres in the database.
    """
    get_genres = """
            SELECT name
            FROM genres
            """
    cursor.execute(get_genres)
    genres = cursor.fetchall()
    return genres

def query_7(search):
    """
    This query returns the movie id, title and overview of the movies that thier title or overview match the search term.
    """
    get_fitting_titles_and_overview = """
            SELECT id, title, overview
            FROM movies
            WHERE MATCH(title, overview) AGAINST (%s IN NATURAL LANGUAGE MODE);
            """
    cursor.execute(get_fitting_titles_and_overview, (search,))
    fitting_titles_and_overview = cursor.fetchall()
    return fitting_titles_and_overview

def query_8(actor_name):
    """
    This query returns the actor's name, and the movie titles that the actor was in
    """
    get_movies_of_actor = """
                        SELECT a.id, m.title
                        FROM actors a 
                        join actor_movies am on a.id = am.actor_id
                        join movies m on am.movie_id = m.id
                        WHERE MATCH(name) AGAINST (%s IN NATURAL LANGUAGE MODE);
                        """
    cursor.execute(get_movies_of_actor, (actor_name,))
    movies_of_actor = cursor.fetchall()
    return movies_of_actor
