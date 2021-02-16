import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    This function drops tables defined in drop_table_queries string from the redshift database
    Input parameters
        cur: cursor pointer to the database
        conn: connection pointer to the database
    Returns: No return value
    '''
    for query in drop_table_queries:
        print("Running drop query ",query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    This function creates tables defined in create_table_queries string from the redshift database
    Input parameters
        cur: cursor pointer to the database
        conn: connection pointer to the database
    Returns: No return value
    '''
    for query in create_table_queries:
        print("Running create query ",query)
        cur.execute(query)
        conn.commit()


def main():
    '''
    The main block - connects to the redshift database and calls relevant functions 
                    to drop and create tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print ("Connecting to cluster")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print ("Connected to cluster")
    cur = conn.cursor()
    

    print("Dropping tables")
    drop_tables(cur, conn)
    
    print("Creating tables")
    create_tables(cur, conn)

    print("Closing connection")
    conn.close()


if __name__ == "__main__":
    main()