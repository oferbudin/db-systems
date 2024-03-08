from create_db_script import db, HOST, USER, PASSWORD, DATABASE, PORT
import mysql.connector

def insert_recordsto_db(table, mydb, cursor, verbose = False):
        cmd = f"INSERT INTO {table.name} ("
        for column in table.columns:
            cmd += f"{column.name},"
        cmd = cmd[:-1]
        cmd += ") VALUES ("
        for i in range(len(table.columns)):
            cmd += "%s,"
        cmd = cmd[:-1]
        cmd += ")"
        if verbose:
            print(cmd)
        cursor.executemany(cmd, table.get_records(table.data_file_path).values.tolist())
        mydb.commit()

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
        for table in db.tables:
            insert_recordsto_db(table, mydb, cursor, verbose)
    except mysql.connector.errors.ProgrammingError as e:
        print("Error: Can't insert records due to invalid command: %s" % e)
    

if __name__ == "__main__":
    main()