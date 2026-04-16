from airflow.providers.postgres.hooks.postgres import postgresHook
from pyscopg2.extras import RealDictCurdor

table = "yt_api"

def get_conn_cursor():
    hook  = postgresHook(postgres_conn_id ="postgres_db_yt_elt", database = "elt_db" )
    conn = hook.get.conn()
    cur = conn.cursor(cursor_factory=RealDictCurdor)

def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()

def create_shema(shema):

    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)

    conn.commit()

    close_conn_cursor(conn, cur)

def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    "Video_ID VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration"  VARCHAR(20) NOTNULL,
                    "Video_Views" INT,
                    "Likes_count" INT,
                    "Comments_Count" INT
                
                );
            """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table}(
                    "Video_ID VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration"  VARCHAR(20) NOT NULL,
                    "Video_Type" VARCHAR(10) NOT NULL,
                    "Video_Views" INT,
                    "Likes_count" INT,
                    "Comments_Count" INT
                
                );
            """
        
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def get_video_ids(cur, shema):

    cur.execute(f"""SELECT Video_ID FROM {schema}.{table};""") 
    ids = cur.fetchall()


    



