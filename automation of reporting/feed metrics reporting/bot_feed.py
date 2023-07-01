# импорт необходимых библотек для работы с данными
import telegram
import pandahouse as ph
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
#import asyncio
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
    'retries': 3, # количество запусков, в случае неудачного выполнения
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
def bot_report_ktupikov():
    
    @task
    def get_data():
        query = '''
            select toDate(time) as date, 
                count(distinct user_id) as dau,
                countIf(action='like') as likes,
                countIf(action='view') as views,
                round(likes/views*100, 2) as ctr
            from simulator_20230520.feed_actions
            where toDate(time) between today() - 7 and today() - 1
            group by toDate(time)
            order by date  
            '''
        data = ph.read_clickhouse(query, connection=connection)
        
        return data
    
    @task
    def create_message(data):
        # отчет за пред. день
        yesterday = pd.to_datetime(data['date'].values[-1]).strftime("%Y-%m-%d")
        start_date = pd.to_datetime(data['date'].values[0]).strftime("%Y-%m-%d")
        cur_dau = data['dau'].values[-1]
        cur_likes = data['likes'].values[-1]
        cur_views = data['views'].values[-1]
        cur_ctr = data['ctr'].values[-1]


        message = f'<b>Hello! Оперативные метрики ленты новостей за {yesterday}:</b>'+'\n'+\
        '<pre>'+'\n'+\
        f'|--------|----------|'+'\n'+\
        f'| likes  | {str(cur_likes).rjust(8, " ")} |'+'\n'+\
        f'| views  | {str(cur_views).rjust(8, " ")} |'+'\n'+\
        f'| DAU    | {str(cur_dau).rjust(8, " ")} |'+'\n'+\
        f'| CTR    | {(str(cur_ctr)+"%").rjust(8, " ")} |'+'\n'+\
        f'|--------|----------|'+'\n'+\
        '</pre>'
        
        return message
    
    @task
    def create_chart(data):
        yesterday = pd.to_datetime(data['date'].values[-1]).strftime("%Y-%m-%d")
        start_date = pd.to_datetime(data['date'].values[0]).strftime("%Y-%m-%d")
        
        sns.set_style("darkgrid")
        fig, axs = plt.subplots(2,2, figsize=(14,9))
        fig.suptitle(f'Оперативные метрики с {start_date} по {yesterday}', fontsize=24)
        sns.lineplot(ax=axs[0,0], x=pd.to_datetime(data['date'].values).strftime("%d-%m"), y=data['dau'], label='dau', marker="o").set(ylabel=None, xlabel=None)
        sns.lineplot(ax=axs[0,1], x=pd.to_datetime(data['date'].values).strftime("%d-%m"), y=data['ctr'], label='ctr', marker="o").set(ylabel=None, xlabel=None)
        sns.lineplot(ax=axs[1,0], x=pd.to_datetime(data['date'].values).strftime("%d-%m"), y=data['likes'], label='likes', marker="o").set(ylabel=None, xlabel=None)
        sns.lineplot(ax=axs[1,1], x=pd.to_datetime(data['date'].values).strftime("%d-%m"), y=data['views'], label='views', marker="o").set(ylabel=None, xlabel=None)
        plt.legend()

        # save graph
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'fig.png'
        plt.close()
        
        return plot_object
    
    @task
    def send_statistic(message, plot):
        bot_send_info(chat_id, message, plot)
        
    
    data = get_data()
    message = create_message(data)
    plot = create_chart(data)
    send_statistic(message, plot)
    
bot_report_ktupikov = bot_report_ktupikov()