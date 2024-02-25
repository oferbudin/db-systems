import pandas as pd
import mysql.connector
from datetime import datetime


HOST = "localhost"
USER = "dbeaver"
PASSWORD = "dbeaver"
DATABASE = "project"

mydb = mysql.connector.connect(
  host=HOST,
  user=USER,
  password=PASSWORD,
  database=DATABASE
)

tables = []

mycursor = mydb.cursor()

def load_data(data_file_name):
    return pd.read_csv(data_file_name)

class Column:
    def __init__(self, name: str, _type: str, converter_funtion = None):
        self.name = name
        self.type = _type
        self.converter_funtion = converter_funtion


class Table:
    def __init__(self, mycursor, name: str, csv_path, columns: list[Column]):
        self.name = name
        self.data = load_data(csv_path)
        self.columns = columns
        self._mycursor = mycursor
    
    def get_records(self):
        records = []
        for column in self.columns:
            records.append(self.data[column.name].apply(column.converter_funtion) if column.converter_funtion else self.data[column.name])
        return pd.DataFrame(records).T
    
    def get_columns(self):
        return self.columns

    def cretae_table_command(self):
        cmd = f"CREATE TABLE IF NOT EXISTS {self.name} ("
        for column in self.columns:
            cmd += f"{column.name} {column.type},"
        cmd = cmd[:-1]
        cmd += ")"
        self._mycursor.execute(cmd)

    def insert_records(self):
        cmd = f"INSERT INTO {self.name} ("
        for column in self.columns:
            cmd += f"{column.name},"
        cmd = cmd[:-1]
        cmd += ") VALUES ("
        for i in range(len(self.columns)):
            cmd += "%s,"
        cmd = cmd[:-1]
        cmd += ")"
        self._mycursor.executemany(cmd, self.get_records().values.tolist())
        mydb.commit()


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
        mycursor,
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
        mycursor,
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
        mycursor,
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
        mycursor,
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
        mycursor,
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
        mycursor,
        "films",
        'films.csv',
        [
            Column("id", "INT"),
            Column("imdb_id", "VARCHAR(255)"),
            Column("title", "VARCHAR(255)"),
            Column("overview", "TEXT"),
            Column("release_date", "DATE", lambda x: (datetime.strptime(x, "%d/%m/%Y") if '/' in x else datetime.strptime(x, "%Y-%m-%d").strftime("%Y-%m-%d"))),
            Column("runtime", "INT"),
            Column("original_language", "VARCHAR(255)"),
            Column("gener", "INT"),
        ]
    )
)

if __name__ == "__main__":
    for table in db.get_tables():
        table.cretae_table_command()
