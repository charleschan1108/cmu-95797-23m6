import duckdb

def main():
    # connect to main.db database where all the data are ingested
    con = duckdb.connect(database = "../sql/main.db", read_only = True)

    # Fetch all table names from main.db
    con.execute("SHOW TABLES")
    tables = con.fetchall()

    # Loop through all tables and count number of rows
    for table in tables:
        tablename = table[0] # results are in tuples, extract table name from tuples
        con.execute(f"SELECT COUNT(*) AS RAW_COUNT FROM {tablename}")
        row_count = con.fetchall()
        print(f"{tablename},{row_count[0][0]}") # print result



if __name__ == "__main__":
    main()