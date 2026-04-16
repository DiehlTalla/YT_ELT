import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cur,conn,schema,row):

    try:

        if schema == 'stagging':
            video_id = 'video_id'

            cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID, "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_count", "Comments_Count" )
                VALUES (%(video_id)s, %(title)s, ù(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s, ;
                """, row
            )

        else:

            video_id = 'Viedeo_ID'

            cur.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID, "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_count", "Comments_Count" )
                VALUES (%(video_id)s, %(title)s, ù(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s, ;
                """, row
            )

        conn.commit()

        logger.info(f"Inserted row with Video_ID: {row[video_id]}")  

    except Exception as e:
        logger.error(f"error inserting row with Video_ID: {row[video_id]}") 
        raise e  


def update_rows(cur,conn,schema,row):

    try:
         #staging 

        if schema == 'stagging':
            video_id = 'video_id'
            upload_date = 'publishedAt',
            video_title = 'title',
            video_views = 'viewCount',
            likes_count = 'likeCount',
            comments_count = 'comments_Count'
          # core 
        else:
            video_id = 'video_id'
            upload_date = 'publishedAt',
            video_title = 'title',
            video_views = 'viewCount',
            likes_count = 'likeCount',
            comments_count = 'comments_Count'

        
            cur.execute( 
                f"""
                UPDATE {schema}.{table}
                SET "video_title" = %({video_title})s,
                     "video_Views" = %({video_views})s,
                     "Likes_Count" = %({likes_count})s,
                     "Comments_Count" = %({comments_count})s,
                WHERE "video_id = %({video_id}) AND "upload_date" = %({upload_date})  
                """, row 
            )

            conn.commit()   

        logger.info(f"Inserted row with Video_ID: {row[video_id]}")  

    except Exception as e:
        logger.error(f"error inserting row with Video_ID: {row[video_id]}") 
        raise e 

def delete_row(cur, conn, schema, ids_to_delete):
    try:

        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
             f"""
             DELETE FROM {schema}.{table}
             WHERE "Video_ID" IN {ids_to_delete};
             """
        )

        conn.commit()   
        logger.info(f"Delete rows with  Video_ID: {ids_to_delete}")  

    except Exception as e:
        logger.error(f"error deleting row with Video_ID: {ids_to_delete} - {e}") 
        raise e 
               
                 
