import pandas as pd
import mysql.connector
from datetime import datetime


HOST = "localhost"
USER = "dbeaver"
PASSWORD = "dbeaver"
DATABASE = "project"
PORT = 3306

def load_data(data_file_name):
    return pd.read_csv(data_file_name)

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
    def __init__(self, name: str, csv_path, columns: list[Column], primary_kies = None, foreign_keys: list[ForeignKey] = None):
        self.name = name
        self.columns = columns
        self.data_file_path = csv_path
        self.primary_kies = primary_kies
        self.foreign_keys = foreign_keys

    
    def get_records(self, data_file_path: str):
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
        if self.primary_kies:
            cmd += f",PRIMARY KEY ({','.join(self.primary_kies)}),"
            cmd = cmd[:-1]
        if self.foreign_keys:
            for foreign_key in self.foreign_keys:
                cmd += f",FOREIGN KEY ({foreign_key.column_name}) REFERENCES {foreign_key.table}({foreign_key.foreign_column_name})"
        cmd += ")"
        cursor.execute(cmd)

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
        primary_kies = ["id"]
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
        ],
        primary_kies = ["id"],
        # foreign_keys = [ForeignKey("genres", "id")]
    )
)
db.add_table(
    Table(
        "actors",
        'actors.csv',
        [
            Column("id", "INT "),
            Column("name", "VARCHAR(255)"),
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
        primary_kies = ["movie_id", "actor_id"],
        # foreign_keys = [ForeignKey("movies", "movie_id", "id"), ForeignKey("actors", "actor_id", "id")]
        # foreign_keys = [ForeignKey("movies", "movie_id", "id")]

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
        primary_kies = ["id"]
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
        primary_kies = ["movie_id", "crew_id"],
        # foreign_keys = [ForeignKey("movies", "movie_id", "id"), ForeignKey("crew_members", "crew_id", "id")]
    )
)
db.add_table(
    Table(
        "ratings",
        'films.csv',
        [
            Column("movie_id", "INT", key="id"),
            Column("popularity", "FLOAT"),
            Column("vote_average", "FLOAT"),
            Column("vote_count", "INT"),
        ],
        primary_kies=["movie_id"],
        # foreign_keys = [ForeignKey("movies", "movie_id", "id")]
    )
)
db.add_table(
    Table(
        "revenue",
        'films.csv',
        [
            Column("movie_id", "INT", key="id"),
            Column("budget", "INT"),
            Column("revenue", "INT"),
        ],
        primary_kies=["movie_id"],
    )
)

if __name__ == "__main__":
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        port=PORT
    )
    cursor = mydb.cursor()

    for table in db.get_tables():
        table.cretae_table_command(cursor)
