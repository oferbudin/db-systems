import os
import pandas as pd
import mysql.connector
from datetime import datetime


def load_data(data_file_name):
    return pd.read_csv(data_file_name)

def get_user_password():
    path = os.path.join('DOCUMENTATION', 'mysql_and_user_password.txt')
    if not os.path.exists(path):
        path = os.path.join('..', 'DOCUMENTATION', 'mysql_and_user_password.txt')
    data = load_data(path)
    return data['username'][0], data['password'][0]

user, password = get_user_password()

HOST = "127.0.0.1"
USER = user
PASSWORD = password
DATABASE = "oferbudin"
PORT = 3305

print(f'You are using {DATABASE} database | Host: {HOST} | Port: {PORT}')

class Column:
    def __init__(self, name: str, _type: str, converter_funtion = None, key: str = ''):
        self.name = name
        self.key = key if key else name
        self.type = _type
        self.converter_funtion = converter_funtion


class ForeignKey:
    def __init__(self, table, column_name, foreign_column_name = None):
        self.column_name  = column_name
        self.table = table
        self.foreign_column_name = foreign_column_name if foreign_column_name else column_name
        

class Table:
    def __init__(self, name: str, csv_path, columns: list[Column], primary_keys = None, foreign_keys: list[ForeignKey] = None, indexes: list[str] = None):
        self.name = name
        self.columns = columns
        self.data_file_path = os.path.join('data', csv_path)
        self.primary_keys = primary_keys
        self.foreign_keys = foreign_keys
        self.indexes = indexes 

    
    def get_records(self, data_file_path: str):
        if not os.path.exists(data_file_path):
            data_file_path = os.path.join('src', data_file_path)
        data = load_data(data_file_path)
        records = []
        for column in self.columns:
            records.append(data[column.key].apply(column.converter_funtion) if column.converter_funtion else data[column.key])
        return pd.DataFrame(records).T
    
    def get_columns(self):
        return self.columns

    def cretae_table_command(self, cursor):
        cmd = f"CREATE TABLE IF NOT EXISTS {self.name} ("
        for column in self.columns:
            cmd += f"{column.name} {column.type},"
        cmd = cmd[:-1]
        if self.primary_keys:
            cmd += f",PRIMARY KEY ({','.join(self.primary_keys)}),"
            cmd = cmd[:-1]
        if self.foreign_keys:
            for foreign_key in self.foreign_keys:
                cmd += f",FOREIGN KEY ({foreign_key.column_name}) REFERENCES {foreign_key.table}({foreign_key.foreign_column_name})"
        cmd += ")"
        if self.verbose:
            print(cmd)
        cursor.execute(cmd)
        #index creation commands
        if self.indexes:
            for index in self.indexes:
                if self.verbose:
                    print(index)
                cursor.execute(index)
    
    def set_verbose(self, verbose: bool):
        self.verbose = verbose
    
    def drop_table(self, cursor):
        cursor.execute(f"DROP TABLE IF EXISTS {self.name}")

class DB:
    def __init__(self):
        self.tables = []

    def add_table(self, table):
        self.tables.append(table)

    def get_tables(self):
        return self.tables


db = DB()
db.add_table(
    Table(
        "genres",
        'genres.csv',
        [
            Column("id", "INT"),
            Column("name", "VARCHAR(255)")
        ],
        primary_keys = ["id"]
    )
)
db.add_table(
    Table(
        "movies",
        'films.csv',
        [
            Column("id", "INT"),
            Column("title", "VARCHAR(255)"),
            Column("overview", "TEXT"),
            Column("release_date", "DATE", lambda x: (datetime.strptime(x, "%d/%m/%Y") if '/' in x else datetime.strptime(x, "%Y-%m-%d").strftime("%Y-%m-%d"))),
            Column("runtime", "INT"),
            Column("original_language", "VARCHAR(255)"),
            Column("genre", "INT"),
            Column("budget", "INT"),
            Column("revenue", "INT"),
            Column("popularity", "FLOAT"),
            Column("vote_average", "FLOAT"),
            Column("vote_count", "INT"),
        ],
        primary_keys = ["id"],
        foreign_keys = [ForeignKey("genres", "genre", "id")],
        indexes=[
            "ALTER TABLE movies ADD Index (genre);",
            "ALTER TABLE movies ADD Index (runtime);",
            "ALTER TABLE movies ADD Index (title);",
            "ALTER TABLE movies ADD FULLTEXT (title, overview);"
        ]
    )
)
db.add_table(
    Table(
        "actors",
        'actors.csv',
        [
            Column("id", "INT "),
            Column("name", "VARCHAR(255)"),
        ],
        primary_keys= ["id"],
        indexes=[
            "ALTER TABLE actors ADD FULLTEXT(name);"
        ]
    )
)
db.add_table(
    Table(
        "actor_movies",
        'actor_movies.csv',
        [
            Column("movie_id", "INT"),
            Column("actor_id", "INT"),
        ],
        primary_keys = ["movie_id", "actor_id"],
        foreign_keys=[
            ForeignKey("movies", "movie_id", "id"),
            ForeignKey("actors", "actor_id", "id")
        ], 
        indexes=[
            "ALTER TABLE actor_movies ADD Index (actor_id, movie_id);",
        ]
    )
)
db.add_table(
    Table(
        "crew_members",
        'crew_members.csv',
        [
            Column("id", "INT"),
            Column("name", "VARCHAR(255)"),
            Column("job", "VARCHAR(255)"),
        ],
        primary_keys = ["id"]
    )
)
db.add_table(
    Table(
        "crew_members_movies",
        'crew_members_movies.csv',
        [
            Column("movie_id", "INT"),
            Column("crew_id", "INT"),
        ],
        primary_keys = ["movie_id", "crew_id"],
        foreign_keys=[
            ForeignKey("movies", "movie_id", "id"),
            ForeignKey("crew_members", "crew_id", "id")
        ]
    )
)

def main(verbose = False):
    try:
        mydb = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            port=PORT
        )
        cursor = mydb.cursor()
    except mysql.connector.errors.ProgrammingError as e:
        print("Error: Can't connect to MySQL server due to wrong user or password or database name" % e)
    except mysql.connector.errors.InterfaceError:
        print("Error: Can't connect to MySQL server due to worng port number or address")

    try:
        for table in db.get_tables():
            table.set_verbose(verbose)
            table.cretae_table_command(cursor)
    except mysql.connector.errors.ProgrammingError as e:
        print("Error: Can't create table due to invalid command: %s" % e)
    


def drop_tables():
    try:
        mydb = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            port=PORT
        )
    except mysql.connector.errors.ProgrammingError as e:
        print("Error: Can't connect to MySQL server due to wrong user or password or database name" % e)
    except mysql.connector.errors.InterfaceError:
        print("Error: Can't connect to MySQL server due to worng port number or address")
    
    try:
        cursor = mydb.cursor()
        for table in reversed(db.get_tables()):
            table.drop_table(cursor)
    except mysql.connector.errors.ProgrammingError as e:
        print("Error: Can't drop table due to invalid command: %s" % e)

if __name__ == "__main__":
   main()