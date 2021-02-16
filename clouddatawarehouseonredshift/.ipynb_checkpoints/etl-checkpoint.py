import configparser
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn,config):
    '''
    This function loads staging tables in Redshift database as per the copy statements 
    in copy_table_queries string from the json files in S3
    Input parameters
        cur: cursor pointer to the database
        conn: connection pointer to the database
        config: config object with configuration variables to connect to redshift database
    Returns: No return value
    '''
    for query in copy_table_queries:
        print("loading query is "+query)
        ARN=config.get("IAM_ROLE","DWH_ROLE_ARN")
        print("ARN is "+ARN)
        cur.execute(query.format(ARN))
        conn.commit()


def insert_tables(cur, conn):
    '''
    This function loads dimension and fact tables in Redshift database as per the insert statements 
    in insert_table_queries string from the staging tables
    Input parameters
        cur: cursor pointer to the database
        conn: connection pointer to the database
    Returns: No return value
    '''
    for query in insert_table_queries:
        print("Inserting query is "+query)
        cur.execute(query)
        conn.commit()


def main():
    '''
    The main block - connects to the redshift database and calls relevant functions 
                    to copy data into staging tables and then insert data into dimension
                    and fact tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("Connecting to cluster")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading statging tables")
    load_staging_tables(cur, conn,config)
    
    print("Inserting into dim and fact tables")
    insert_tables(cur, conn)

    print("Disconnecting from Redshift")    
    conn.close()


if __name__ == "__main__":
    main()