import configparser
import psycopg2
import boto3
import time
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries

print("Copying source files to S3", flush=True)	

def move_source_files_to_s3():    
	"""Copies the source datasets to S3."""    
	
	config = configparser.ConfigParser()
	config.read_file(open('dl.cfg'))
	
	AWS_ACCESS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
	AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
	AWS_BUCKET = config.get('S3', 'AWS_BUCKET')
	
	s3 = boto3.resource('s3',region_name='us-west-2',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
	
	
	s3.meta.client.upload_file('../world-happiness-report/world-happiness-report.json', AWS_BUCKET, 'world-happiness-report.json')
	s3.meta.client.upload_file('../world-happiness-report/Capitals.csv', AWS_BUCKET, 'Capitals.csv')
	s3.meta.client.upload_file('../world-happiness-report/Cost-Of-Living.csv', AWS_BUCKET, 'Cost-Of-Living.csv')
    


if __name__ == '__main__':
    move_source_files_to_s3()
	
print("successfully uploaded files to S3", flush=True)	
time.sleep(3)

print("Creating Tables in Redshift Cluster", flush=True)	

"""Creating stage and target tables in Redshift cluster"""


"""DROP TABLES SCRIPTS FROM DATABASE"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        print('Executing:' +query)
        cur.execute(query)
        conn.commit()

"""CREATE TABLES SCRIPTS"""
def create_tables(cur, conn):
    for query in create_table_queries:
        print('Executing:' +query)
        cur.execute(query)
        conn.commit()


def Create_drop_tables():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    Create_drop_tables()

print("All tables are created successfully", flush=True)	
time.sleep(3)


print("Loading Data from S3 files to Staging and Target TABLES", flush=True)	

"""Loading Data from S3 files to Staging tables"""

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Copying data from ' +query)
        cur.execute(query)
        conn.commit()

"""Loading Data from Staging tables to Target tables"""

def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Inserting data into' +query)
        cur.execute(query)
        conn.commit()

def Loading():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    Loading()
	
print("Target tables load completed", flush=True)	
