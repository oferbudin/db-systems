from create_db_script import db

def main():
    for table in db.tables:
        table.insert_records()

if __name__ == "__main__":
    main()