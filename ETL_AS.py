import pandahouse as ph
from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_table_by_group():

    query_feed = """
                    SELECT 
                      user_id,
                      gender, 
                      age,
                      os,
                      count(action='like') as likes,
                      count(action='views') as views
                    FROM simulator_20221120.feed_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY user_id, gender, age, os
                                format TSVWithNames
                 """
    query_message = """
SELECT 
  l.day as day,
  l.user_id as user_id,
  l.gender as gender,
  l.age as age,
  l.os as os,
  l.messages_sent as messages_sent,
  l.messages_received as messages_received,
  r.users_sent as users_sent,
  r.users_received as users_received
FROM (
      SELECT 
        l.day as day,
        l.user_id as user_id,
        l.gender as gender,
        l.age as age,
        l.os as os,
        l.messages_sent as messages_sent,
        r.messages_received as messages_received
      FROM (
            SELECT 
              toDate(time) as day,
              user_id,
              gender, 
              age,
              os,
              count() as messages_sent 
            FROM simulator_20221120.message_actions 
            WHERE toDate(time) = yesterday()
            GROUP BY user_id, gender, age, os, day
            ) as l 
      full outer JOIN (
            SELECT 
              reciever_id as user_id,
              count() as messages_received,
              gender, 
              age,
              os,
              toDate(time) as day
            FROM simulator_20221120.message_actions 
            WHERE toDate(time) = yesterday()
            GROUP BY reciever_id, gender, age, os, day
            ) as r 
      ON 
        l.user_id = r.user_id AND
        l.day = r.day AND
        l.age = r.age AND
        l.gender = r.gender AND
        l.os = r.os
      ) as l
full outer JOIN (
      SELECT 
        toDate(yesterday()) as day,
        l.user_id as user_id,
        l.users_sent as users_sent,
        r.users_received as users_received,
        l.age as age, 
        l.gender as gender, 
        l.os as os
      FROM (
            SELECT 
              user_id,
              count() as users_sent,
              age, 
              gender, 
              os
            FROM (
                  SELECT 
                    user_id,
                    COUNT() as sent_mess,
                    reciever_id,
                    age, 
                    gender, 
                    os
                  FROM simulator_20221120.message_actions 
                  WHERE toDate(time) = yesterday()
                  GROUP BY user_id, reciever_id, age, gender, os
                  ORDER BY user_id
                  )
            GROUP BY user_id, age, gender, os
            ORDER BY user_id
            ) as l 
      full outer JOIN (
            SELECT
              p_user_id as user_id,
              count() as users_received,
              age, 
              gender, 
              os
            FROM (
                  SELECT 
                    reciever_id as p_user_id,
                    COUNT() as users_received,
                    user_id as o_user_id,
                    age, 
                    gender, 
                    os
                  FROM simulator_20221120.message_actions 
                  WHERE toDate(time) = yesterday()
                  GROUP BY p_user_id, user_id,age, gender, os
                  ORDER BY o_user_id
                  )
            GROUP BY user_id, age, gender, os
            ORDER BY user_id
            ) as r 
      ON 
        l.user_id = r.user_id AND 
        l.age = r.age AND
        l.gender = r.gender AND
        l.os = r.os 
      ) as r
ON 
  l.user_id = r.user_id AND 
  l.day = r.day AND
  l.age = r.age AND
  l.gender = r.gender AND
  l.os = r.os
WHERE user_id != 0
format TSVWithNames
"""
    query_create_table = """
            CREATE TABLE IF NOT EXIST test.pda_etl_sa (
                event_date Date,
                dimension String,
                dimension_value String,
                likes UInt16,
                views UInt16,
                messages_sent UInt16,
                messages_received UInt16,
                users_sent UInt16,
                users_received UInt16
            )
            ENGINE = MergeTree
            """
    connection_simulator_20221120 = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'database': 'simulator_20221120',
        'user': 'student',
        'password': 'dpo_python_2020'
    }
    connection_test = {
        'database': 'test',
        'host': 'https://clickhouse.lab.karpov.courses',
        'user': 'student-rw',
        'password': '656e2b0c9c'
    }

    @task()
    def extract(query, connection):
        df_cube = ph.read_clickhouse(query, connection=connection)
        return df_cube

    @task
    def transfrom_merge_feed_and_mess(feed, mess):
        df_cube = feed.merge(mess, how='outer', on=['day', 'user_id', 'gender', 'age', 'os'])
        return df_cube

    @task
    def transform_group(df_cube, group):
        df = df_cube.copy(deep=True)

        df = df \
            .groupby(['day', f'{group}'], as_index=False) \
            .agg({'likes': 'sum',
                  'views': 'sum',
                  'messages_sent': 'sum',
                  'messages_received': 'sum'}) \
            .rename(columns={f'{group}': 'dimension_value'})

        df['dimension'] = group
        df = df[['day', 'dimension', 'dimension_value', 'likes', 'views', 'messages_sent', 'messages_received']]

        return df

    @task
    def transform_result_table(os, gender, age):
        return pd.concat([os, gender, age])

    @task
    def load(query_create_table, connection_test, df):
        ph.execute(query=query_create_table, connection=connection_test)
        ph.to_clickhouse(df, 'pda_etl_sa', index=False, connection=connection_test)

    #extract
    feed = extract(query_feed, connection_simulator_20221120)
    mess = extract(query_message, connection_simulator_20221120)

    #transform
    df_cube = transfrom_merge_feed_and_mess(feed, mess)
    os = transform_group(df_cube, 'os')
    gender = transform_group(df_cube, 'gender')
    age = transform_group(df_cube, 'age')
    result = transform_result_table(os, gender, age)

    #load
    load(query_create_table, connection_test, result)

dag_table_by_group = dag_table_by_group()

