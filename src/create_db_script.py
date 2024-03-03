import pandas as pd
import mysql.connector
from datetime import datetime


HOST = "localhost"
USER = "oferbudin"
PASSWORD = "ofe6287"
DATABASE = "oferbudin"
PORT = 3305

def load_data(data_file_name):
    return pd.read_csv(data_file_name)

class Column:
    def __init__(self, name: str, _type: str, converter_funtion = None, key: str = ''):
        self.name = name
        self.key = key if key else name
        self.type = _type
        self.converter_funtion = converter_funtion


class Table:
    def __init__(self, name: str, csv_path, columns: list[Column]):
        self.name = name
        self.columns = columns
        self.data_file_path = csv_path
    
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
        "geners",
        'genres.csv',
        [
            Column("id", "INT"),
            Column("name", "VARCHAR(255)")
        ]
    )
)
db.add_table(
    Table(
        "actors",
        'actors.csv',
        [
            Column("id", "INT"),
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
        ]
    )
)
db.add_table(
    Table(
        "crew_members_movies",
        'crew_members_movies.csv',
        [
            Column("movie_id", "INT"),
            Column("crew_id", "INT"),
            Column("role", "VARCHAR(255)"),
        ]
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
            Column("gener", "INT"),
        ]
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
        ]
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
        ]
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
