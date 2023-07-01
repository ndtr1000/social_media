# импорт необходимых библотек для работы с данными
import telegram
import pandahouse as ph
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
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

# парметры бота
token_bot = '6199475169:AAGeKOGTECFftcp9hJb7O6Px58Nnt1Xw7l8'
#chat_id = 176979642 # my
chat_id = -938659451 #group
bot = telegram.Bot(token=token_bot)

# определяем DAG
@dag(default_args=default_args, schedule_interval=timedelta(minutes=15), catchup=False)
def alert_system_ktupikov():
    
    def check_anomaly(df, metric, a=4, n=6):
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['range'] = df['q75'] - df['q25']
        df['low'] = df['q25'] - a*df['range']
        df['high'] = df['q75'] + a*df['range']

        df['high'] = df['high'].rolling(n, center=True, min_periods=1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

        if df[metric].iloc[-1] > df['high'].iloc[-1] or df[metric].iloc[-1] < df['low'].iloc[-1]:
            return True, df
        else:
            return False, df 
        

    def make_graph(df, metric):
        fig, axs = plt.subplots(figsize=(10,7))
        fig.suptitle(f'Метрика - {metric}')
        sns.lineplot(data=df, x=df['minutes'], y=df[metric], label=metric, color='blue', errorbar=None)
        sns.lineplot(data=df, x=df['minutes'], y=df['high'], label='high bound', color='red', errorbar=None)
        sns.lineplot(data=df, x=df['minutes'], y=df['low'], label='low bound', color='green', errorbar=None)
        
        axs.set_xticks(axs.get_xticks()[::7])

        plt.legend()
        plot = io.BytesIO()
        plt.savefig(plot)
        plot.seek(0)
        plot.name = 'fig.png'
        plt.close()
        
        return plot
    
    # функция для отправки ботом информации
    def bot_send_info(message='', photo=None, chat_id=chat_id):
        bot.sendMessage(chat_id=chat_id, text=message, parse_mode='HTML')
        print('Message sent successfully!')
        bot.sendPhoto(chat_id=chat_id, photo=photo)
        print('Photo sent successfully!')
        
    @task
    def get_data():
        query = '''
        select toDate(time) as date, 
            formatDateTime(toStartOfFifteenMinutes(time), '%R') as minutes,
            count(distinct user_id) as dau,
            countIf(action='like') as likes,
            countIf(action='view') as views
        from simulator_20230520.feed_actions 
        where time >= today() - 1 and time < toStartOfFifteenMinutes(now())
        group by date, minutes
        order by date, minutes
        '''
        data = ph.read_clickhouse(query, connection=connection)

        return data

    @task
    def start_work(data):
        metrics = ['dau', 'likes', 'views']
        for metric in metrics:
            flag, df = check_anomaly(data[['date', 'minutes', metric]].copy(), metric)
            if flag:
                message = f'Обнаружено отклонение!' + '\n' +\
                          f'Метрика {metric}, время {data["minutes"].iloc[-1]}' +'\n' +\
                          f'Текущее значение: {data[metric].iloc[-1]}' +'\n' + \
                          f'Предыдущее значение: {data[metric].iloc[-2]}' +'\n' + \
                          f'Отклонение: {round((data[metric].iloc[-1] - data[metric].iloc[-2])/data[metric].iloc[-2]*100,2)}%'
                plot = make_graph(df, metric)
                bot_send_info(message, plot)

    data = get_data()
    start_work(data)


alert_system_ktupikov = alert_system_ktupikov()
