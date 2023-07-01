# импорт необходимых библотек для работы с данными
import telegram
import pandahouse as ph
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import time as t
from datetime import date, timedelta, datetime

# импортируем инструменты для работы с AirFlow
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры соединения с БД
connection = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'database':'simulator_20230520',
                'user':'student', 
                'password':'dpo_python_2020'
            }

# указываем начальные параметры для нашего DAG'a
default_args = {
    'owner': 'k.tupikov', # владелец
    'depends_on_past': False, # стартует от текущей даты
    'retries': 2, # количество запусков, в случае неудачного выполнения
    'retry_delay': timedelta(minutes=5), #  интервал перезапуска
    'start_date': datetime(2023, 6, 11) # дата начала запуска
}

token_bot = '6199475169:AAGeKOGTECFftcp9hJb7O6Px58Nnt1Xw7l8'
#chat_id = 176979642 # my
chat_id = -938659451 #group
bot = telegram.Bot(token=token_bot)

def bot_send_info(chat_id=None, message='', photo=None):
    bot.sendMessage(chat_id=chat_id, text=message, parse_mode='HTML')
    print('Bot sent message!')
    t.sleep(1)
    bot.sendPhoto(chat_id=chat_id, photo=photo)
    print('Bot sent photo!')


# определяем DAG
@dag(default_args=default_args, schedule_interval='55 10 * * *', catchup=False)
def total_report_ktupikov():
    
    @task
    def get_dau_info():
        query = '''
        with dau_feed_info as (
            select date, dau as dau_feed,
                round((dau - prev_dau) / prev_dau * 100,1) as change_rate_feed
            from (
                select date, dau,
                any(dau) over(rows between 1 preceding and 1 preceding) as prev_dau
                from (
                select date, count(distinct user_id) as dau
                from simulator_20230520.feed_actions
                where toDate(time) between today() - 7 and today() - 1
                group by toDate(time) as date
                ) as t1
            ) as t2
        ),
        dau_message_info as (
            select date, dau as dau_message, 
                round((dau - prev_dau) / prev_dau * 100,1) as change_rate_message
            from (
                select date, dau,
                any(dau) over(rows between 1 preceding and 1 preceding) as prev_dau
                from (
                select date, count(distinct user_id) as dau
                from simulator_20230520.message_actions 
                where toDate(time) between today() - 7 and today() - 1
                group by toDate(time) as date
                ) as t1
            ) as t2
        ),
        dau_both_info as (
            select date, dau as dau_both, 
                round((dau - prev_dau) / prev_dau * 100,1) as change_rate_both
            from (
                select date, dau,
                any(dau) over(rows between 1 preceding and 1 preceding) as prev_dau
                from (
                select date, count(distinct user_id) as dau
                from simulator_20230520.feed_actions full outer join simulator_20230520.message_actions using(time, user_id)
                where toDate(time) between today() - 7 and today() - 1 
                group by toDate(time) as date
                ) as t1
            ) as t2
        )

        select date, dau_feed, change_rate_feed, dau_message, change_rate_message, dau_both, change_rate_both
        from (
        select dau_feed_info.date as date, dau_feed, change_rate_feed, dau_message, change_rate_message
        from dau_feed_info join dau_message_info using(date)
        ) as t join dau_both_info using(date)
        '''
        dau_info = ph.read_clickhouse(query, connection=connection)

        return dau_info
    @task
    def get_new_users_info():
        query = '''
        with new_users_feed as (
            select date, new_users_organic_feed, sum_new_users_organic_feed, new_users_ads_feed, sum_new_users_ads_feed,
                round(((new_users_organic_feed+new_users_ads_feed) - p_f)/p_f * 100,2) as change_rate_feed
            from (
                select date, new_users_organic_feed, sum_new_users_organic_feed, new_users_ads_feed, sum_new_users_ads_feed,
                any(new_users_organic_feed+new_users_ads_feed) over(rows between 1 preceding and 1 preceding) as p_f
                from (
                select first_action as date,
                    new_users_organic as new_users_organic_feed, sum(new_users_organic_feed) over(order by date) as sum_new_users_organic_feed,
                    new_users_ads as new_users_ads_feed, sum(new_users_ads_feed) over(order by date) as sum_new_users_ads_feed
                from (
                    select first_action,
                    countIf(source='organic') as new_users_organic,
                    countIf(source='ads') as new_users_ads
                    from (
                    select user_id, source, min(toDate(time)) as first_action
                    from simulator_20230520.feed_actions 
                    where toDate(time) between today() - 7 and today() - 1
                    group by user_id, source
                    ) 
                    group by first_action
                )
                )  
            )
        ),
        new_users_message as (
            select date, new_users_organic_mes, sum_new_users_organic_mes, new_users_ads_mes, sum_new_users_ads_mes,
                round(((new_users_organic_mes+new_users_ads_mes) - p_f)/p_f * 100,2) as change_rate_mes
            from (
                select date, new_users_organic_mes, sum_new_users_organic_mes, new_users_ads_mes, sum_new_users_ads_mes,
                any(new_users_organic_mes+new_users_ads_mes) over(rows between 1 preceding and 1 preceding) as p_f
                from (
                select first_action as date,
                    new_users_organic as new_users_organic_mes, sum(new_users_organic_mes) over(order by date) as sum_new_users_organic_mes,
                    new_users_ads as new_users_ads_mes, sum(new_users_ads_mes) over(order by date) as sum_new_users_ads_mes
                from (
                    select first_action,
                    countIf(source='organic') as new_users_organic,
                    countIf(source='ads') as new_users_ads
                    from (
                    select user_id, source, min(toDate(time)) as first_action
                    from simulator_20230520.message_actions
                    where toDate(time) between today() - 7 and today() - 1
                    group by user_id, source
                    ) 
                    group by first_action
                )  
                )
            )
        )

        select date, new_users_organic_feed, sum_new_users_organic_feed, new_users_ads_feed, sum_new_users_ads_feed, change_rate_feed,
            new_users_organic_mes, sum_new_users_organic_mes, new_users_ads_mes, sum_new_users_ads_mes, change_rate_mes
        from new_users_feed join new_users_message using(date)
        '''
        new_users_info = ph.read_clickhouse(query, connection=connection)

        return new_users_info
    
    @task
    def get_enother_info():
        query = '''
        with os_usage as (
            select t1.date as date, 
                round(iOS / (iOS + Android) * 100,1) as iOS_r,
                round(Android / (iOS + Android) * 100,1) as Android_r
            from (
                select toDate(time) as date, count(os) as iOS
                from simulator_20230520.feed_actions 
                where (toDate(time) between today() - 7 and today() - 1) and os='iOS'
                group by toDate(time)
            ) as t1 inner join (
                select toDate(time) as date, count(os) as Android
                from simulator_20230520.feed_actions 
                where (toDate(time) between today() - 7 and today() - 1) and os='Android'
                group by toDate(time)
            ) as t2 using(date)
        ),
        trafic_usage as (
            select date,
                round(ads / (ads + organic) * 100,1) as ads_r,
                round(organic / (ads + organic) * 100,1) as organic_r
            from (
                select toDate(time) as date, count(os) as ads
                from simulator_20230520.feed_actions 
                where (toDate(time) between today() - 7 and today() - 1) and source='ads'
                group by toDate(time)
            ) as t1 inner join (
                select toDate(time) as date, count(os) as organic
                from simulator_20230520.feed_actions 
                where (toDate(time) between today() - 7 and today() - 1) and source='organic'
                group by toDate(time)
            ) as t2 using(date)
        )

        select date, iOS_r, Android_r, ads_r, organic_r
        from os_usage join trafic_usage using(date)
        '''
        enother_info = ph.read_clickhouse(query, connection=connection)

        return enother_info
    
    @task
    def get_statistic_info():
        query = '''
        with feed_info as (
            select date, 
                likes, round((likes-prev_l)/prev_l*100,2) as likes_change,
                views, round((views-prev_v)/prev_v*100,2) as views_change
            from (
                select date, 
                likes, any(likes) over(rows between 1 preceding and 1 preceding) as prev_l,
                views, any(views) over(rows between 1 preceding and 1 preceding) as prev_v
                from (
                select toDate(time) as date, 
                    countIf(action='like') as likes,
                    countIf(action='view') as views
                from simulator_20230520.feed_actions 
                where toDate(time) < today()
                group by toDate(time)
                ) as t1
            )
        ),
        messages_info as (
            select date, send_messages, 
                round((send_messages-prev_m)/prev_m*100,2) as messages_change
            from (
                select date, send_messages,
                any(send_messages) over(rows between 1 preceding and 1 preceding) as prev_m
                from (
                select toDate(time) as date, 
                    count(*) as send_messages
                from simulator_20230520.message_actions
                where toDate(time) < today()
                group by toDate(time)
                ) as t1
            )
        )

        select date, likes, likes_change, views, views_change, send_messages, messages_change
        from feed_info join messages_info using(date)
        '''
        stat_info = ph.read_clickhouse(query, connection=connection)

        return stat_info
    
    @task
    def create_message(dau_info, new_users_info, stat_info):
        yesterday = pd.to_datetime(dau_info['date'].values[-1]).strftime("%Y-%m-%d")
        start_date = pd.to_datetime(dau_info['date'].values[0]).strftime("%Y-%m-%d")

        dau_feed = dau_info['dau_feed'].values[-1]
        dau_messages = dau_info['dau_message'].values[-1]
        dau_both = dau_info['dau_both'].values[-1]
        dau_feed_change = dau_info['change_rate_feed'].values[-1]
        dau_messages_change = dau_info['change_rate_message'].values[-1]
        dau_both_change = dau_info['change_rate_both'].values[-1]

        new_users_feed = new_users_info['new_users_organic_feed'].values[-1]+new_users_info['new_users_ads_feed'].values[-1]
        new_users_messages = new_users_info['new_users_organic_mes'].values[-1]+new_users_info['new_users_ads_mes'].values[-1]
        new_users_feed_change = new_users_info['change_rate_feed'].values[-1]
        new_users_messages_change = new_users_info['change_rate_mes'].values[-1]

        likes = stat_info['likes'].values[-1]
        views = stat_info['views'].values[-1]
        messages = stat_info['send_messages'].values[-1]
        likes_change = stat_info['likes_change'].values[-1]
        views_change = stat_info['views_change'].values[-1]
        messages_change = stat_info['messages_change'].values[-1]

        message = f'<b>Hello! Общая статистика c {start_date} по {yesterday}:</b>'+'\n'+\
            '<pre>'+'\n'+\
            f'|      Info    | Value|   %  |'+'\n'+\
            f'|--------------|------|------|'+'\n'+\
            f'|DAU Feed      |{str(dau_feed).rjust(6, " ")}|{str(dau_feed_change).rjust(6, " ")}|'+'\n'+\
            f'|DAU Message   |{str(dau_messages).rjust(6, " ")}|{str(dau_messages_change).rjust(6, " ")}|'+'\n'+\
            f'|DAU Both      |{str(dau_both).rjust(6, " ")}|{str(dau_both_change).rjust(6, " ")}|'+'\n'+\
            f'|--------------|------|------|'+'\n'+\
            f'|New Users Feed|{str(new_users_feed).rjust(6, " ")}|{str(new_users_feed_change).rjust(6, " ")}|'+'\n'+\
            f'|New Users Mess|{str(new_users_messages).rjust(6, " ")}|{str(new_users_messages_change).rjust(6, " ")}|'+'\n'+\
            f'|--------------|------|------|'+'\n'+\
            f'|Likes         |{str(likes).rjust(6, " ")}|{str(likes_change).rjust(6, " ")}|'+'\n'+\
            f'|Views         |{str(views).rjust(6, " ")}|{str(views_change).rjust(6, " ")}|'+'\n'+\
            f'|Messages      |{str(messages).rjust(6, " ")}|{str(messages_change).rjust(6, " ")}|'+'\n'+\
            f'|--------------|------|------|'+'\n'+\
            '</pre>'
        return message
    
    @task
    def create_chart(new_users_info, enother_info):
        dates_users = pd.to_datetime(new_users_info['date'].values).strftime("%d-%m")
        dates_info = pd.to_datetime(enother_info['date'].values).strftime("%d-%m")
        fig, axs = plt.subplots(3,2, figsize=(14,11))
        start_date = pd.to_datetime(new_users_info['date'].values[0]).strftime("%Y-%m-%d")
        yesterday = pd.to_datetime(new_users_info['date'].values[-1]).strftime("%Y-%m-%d")
        fig.suptitle(f'Общая статистика с {start_date} по {yesterday}', fontsize=18, fontweight='bold')
        sns.set_theme()
        sns.lineplot(ax=axs[0,0], x=dates_users, y=new_users_info['sum_new_users_ads_feed']+new_users_info['sum_new_users_organic_feed'], label='feed').set(title='Общее количество пользователей Ленты Новостей',xlabel=None, ylabel=None)
        axs[0,1].bar(dates_users, new_users_info['new_users_organic_feed'], color='red', label='organic', alpha=0.7)
        axs[0,1].bar(dates_users, new_users_info['new_users_ads_feed'], color='blue', bottom=new_users_info['new_users_organic_feed'], label='ads', alpha=0.7)
        axs[0,1].legend()
        axs[0,1].set_title('Прирост пользователей Ленты Новостей')
        sns.lineplot(ax=axs[1,0], x=dates_users, y=new_users_info['sum_new_users_ads_mes']+new_users_info['sum_new_users_organic_mes'], label='message').set(title='Общее количество пользователей Мессенджера', xlabel=None, ylabel=None)
        axs[1,1].bar(dates_users, new_users_info['new_users_organic_mes'], color='red', label='organic', alpha=0.7)
        axs[1,1].bar(dates_users, new_users_info['new_users_ads_mes'], color='blue', bottom=new_users_info['new_users_organic_mes'], label='ads', alpha=0.7)
        axs[1,1].legend()
        axs[1,1].set_title('Прирост пользователей Мессенджера')
        sns.lineplot(ax=axs[2,0], x=dates_info, y=enother_info['iOS_r'], label='iOS').set(title='Трафик по ОС', ylabel=None,xlabel=None)
        sns.lineplot(ax=axs[2,0], x=dates_info, y=enother_info['Android_r'], label='Android').set( ylabel=None,xlabel=None)
        sns.lineplot(ax=axs[2,1], x=dates_info, y=enother_info['ads_r'], label='ads').set(title='Трафик по источникам', ylabel=None,xlabel=None)
        sns.lineplot(ax=axs[2,1], x=dates_info, y=enother_info['organic_r'], label='organic').set( ylabel=None,xlabel=None)
    
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'fig.png'
        plt.close()

        return plot_object
    
    @task
    def bot_send_report(chat_id, message, plot_object):
        bot_send_info(chat_id, message, plot_object)
    
    dau_info = get_dau_info()
    new_users_info = get_new_users_info()
    enother_info = get_enother_info()
    stat_info = get_statistic_info()
    message = create_message(dau_info, new_users_info, stat_info)
    plot_object = create_chart(new_users_info, enother_info)
    bot_send_report(chat_id, message, plot_object)

total_report_ktupikov = total_report_ktupikov()
