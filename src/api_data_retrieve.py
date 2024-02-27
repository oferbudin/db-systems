from create_db_script import db, HOST, USER, PASSWORD, DATABASE
import mysql.connector

def insert_recordsto_db(table, mydb, cursor):
        cmd = f"INSERT INTO {table.name} ("
        for column in table.columns:
            cmd += f"{column.name},"
        cmd = cmd[:-1]
        cmd += ") VALUES ("
        for i in range(len(table.columns)):
            cmd += "%s,"
        cmd = cmd[:-1]
        cmd += ")"
        cursor.executemany(cmd, table.get_records(table.data_file_path).values.tolist())
        mydb.commit()

def main():
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )
    cursor = mydb.cursor()

    for table in db.tables:
        insert_recordsto_db(table, mydb, cursor)

if __name__ == "__main__":
    main()