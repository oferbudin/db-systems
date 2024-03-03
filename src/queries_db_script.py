from create_db_script import db, HOST, USER, PASSWORD, DATABASE, PORT
import mysql.connector

def example_all_movies(cursor):
    cursor.execute("SELECT * FROM movies")
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