# импорт необходимых библотек для работы с данными
import pandas as pd
import pandahouse as ph
from datetime import date, timedelta, datetime

# импортируем инструменты для работы с AirFlow
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# указываем начальные параметры для нашего DAG'a
default_args = {
    'owner': 'k.tupikov', # владелец
    'depends_on_past': False, # стартует от текущей даты
    'retries': 3, # количество запусков, в случае неудачного выполнения
    'retry_delay': timedelta(minutes=5), #  интервал перезапуска
    'start_date': datetime(2023, 6, 11) # дата начала запуска
}

# определяем параметры для чтения с БД
connection = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'database':'simulator_20230520',
                'user':'student', 
                'password':'dpo_python_2020'
            }

# определяем параметры для записи в БД
connection_load = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'database': 'test',
            'user': 'student-rw',
            'password': '656e2b0c9c'
        }

# определяем DAG
@dag(default_args=default_args, schedule_interval='0 1 * * *', catchup=False)
def daily_info():
    
    @task
    def extract_feed_info():
        # для каждого пользователя считаем сколько он посмотрел постов и лайков, а также его os, age, gender
        query_f = '''
                select user_id,
                  countIf(action='like') as likes,
                  countIf(action='view') as views,
                  gender,
                  age_group,
                  os
                from (
                  select user_id, action,
                    if(gender=1,'male','female') as gender,
                    multiIf(
                      age<25, 'below 25',
                      age between 25 and 35, '25-35',
                      age between 36 and 50, '36-50',
                      '50+') as age_group,
                    os
                  from simulator_20230520.feed_actions
                  where toDate(time)=today()-1
                )
                group by user_id, gender, age_group, os
            '''
        # Выполняем запрос к БД
        df_feed_actions = ph.read_clickhouse(query_f, connection=connection)
        
        return df_feed_actions
    
    @task
    def extract_message_info():
        # для каждого пользователя считаем сколько он отправил сообщений, скольким людям отправил сообщения, сколько сообщений получил, от скольких людей получал сообщения
        # для решения этой задачи необходимо будем выполнить два подзапроса, сначала для работы с отправкой сообщений и с поулчением сообщений
        # т.к. в нашем случае пользователь может только отправлять сообщения и не получить ниодного, также справедливо и наоборот, поэтому при join's двух подзапросов используется полное внешнее соединение, т.к. нам важно чтобы попали все пользователи из двух подзапросов
        query_m = '''
            with messages_statistic as (
              select user_id, messages_sent, users_sent, messages_received, users_received
              from (
                -- 1. Сколько пользователь отсылает сообщений и скольким людям
                select user_id,
                  count(user_id) as messages_sent,
                  count(distinct reciever_id) as users_sent
                from simulator_20230520.message_actions
                where toDate(time) = today()-1
                group by user_id
              ) as t1 full outer join (
                -- 2. Сколько пользователь получает сообщений и сколько людей ему пишет
                select reciever_id as user_id, messages_received, users_received
                from (
                  select reciever_id, count(*) as messages_received,
                    count(distinct user_id) as users_received
                  from simulator_20230520.message_actions
                  where toDate(time) = today()-1
                  group by reciever_id
                ) as t3
              ) as t2 using(user_id)
            )

            select distinct messages_statistic.user_id, messages_sent, users_sent, messages_received, users_received,
              if(gender=1,'male','female') as gender,
              multiIf(
                age<25, 'below 25',
                age between 25 and 35, '25-35',
                age between 36 and 50, '36-50',
              '50+') as age_group,
              os
            from messages_statistic left join simulator_20230520.feed_actions using(user_id)
        '''
        # Выполняем запрос к БД
        df_message = ph.read_clickhouse(query_m, connection=connection)
        
        return df_message
    
    @task
    def merge_table(table1, table2):
        # при объединение информации о пользователях может возникнуть ситуация, когда пользователь только смотрел или лайкал посты, но не получал сообщений и не отправлял, также справедливо и наоборот. Для того, чтобы не потерять таких пользователь используем полное внешнее соединение.
        # также после объединения могут появится NaN, из за того, что какие то пользователи находятся только в одной из таблиц. Для таких пользователей мы заполним их статистику нулем.
        merged_df = table1.merge(table2, how='outer', on=['user_id','gender','age_group','os'])
        merged_df.fillna(0, inplace=True)
        
        return merged_df
    
    @task
    def dimension_gender(table):
        gender_df = table.groupby('gender')[['likes','views','messages_sent','users_sent','messages_received','users_received']].sum()\
                        .reset_index()\
                        .rename(columns={'gender':'dimension_value'})
        gender_df.insert(loc=0, column='dimension', value='gender')
        
        return gender_df
    
    @task
    def dimension_age(table):
        age_group_df = table.groupby('age_group')[['likes','views','messages_sent','users_sent','messages_received','users_received']].sum()\
                        .reset_index()\
                        .rename(columns={'age_group':'dimension_value'})
        age_group_df.insert(loc=0, column='dimension', value='age_group')
        
        return age_group_df
    
    @task
    def dimension_os(table):
        os_df = table.groupby('os')[['likes','views','messages_sent','users_sent','messages_received','users_received']].sum()\
                        .reset_index()\
                        .rename(columns={'os':'dimension_value'})
        os_df.insert(loc=0, column='dimension', value='os')
        
        return os_df
    
    @task
    def merge_final_table(table1, table2, table3):
        final_table = pd.concat([table1, table2, table3], ignore_index=True)
        yesterday = date.today() - timedelta(days=1)
        final_table.insert(loc=0, column='event_date', value=yesterday.strftime('%Y-%m-%d'))
        final_table = final_table[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        final_table['views'] = final_table['views'].apply(int)
        final_table['likes'] = final_table['likes'].apply(int)
        final_table['messages_received'] = final_table['messages_received'].apply(int)
        final_table['messages_sent'] = final_table['messages_sent'].apply(int)
        final_table['users_received'] = final_table['users_received'].apply(int)
        final_table['users_sent'] = final_table['users_sent'].apply(int)
        
        return final_table
        
    @task
    def load_info(df):
        # для записи в БД нам необходима таблица, создадим ее. 
        create_table_query = '''
            create table if not exists test.ktupikov_daily_etl
            (
                event_date Date,
                dimension String,
                dimension_value String,
                views UInt32,
                likes UInt32,
                messages_received UInt32,
                messages_sent UInt32,
                users_received UInt32,
                users_sent UInt32
            )
            ENGINE = Log()
        '''
        ph.execute(create_table_query, connection=connection_load)
        print('create table successful!')
        print('-------------------------')
        ph.to_clickhouse(df, table='ktupikov_daily_etl', connection=connection_load, index=False)
        print('table successfully update!')
        
    df_feed_actions = extract_feed_info()
    df_message = extract_message_info()
    merged_df = merge_table(df_feed_actions, df_message)
    gender_df = dimension_gender(merged_df)
    age_group_df = dimension_age(merged_df)
    os_df = dimension_os(merged_df)
    final_table = merge_final_table(gender_df, age_group_df, os_df)
    load_info(final_table)
    
daily_info = daily_info()