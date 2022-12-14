import telegram
from datetime import timedelta
import datetime
from airflow.decorators import dag, task
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import io

sns.set(font_scale=1)
#Параметры бота
token = '5937135186:AAEP_nEJ3HJ8-k7mQYQ5HSTTP67ZwVHBMUE'
bot = telegram.Bot(token=token)

#Параметры для отчета
query_yesterday = """
                SELECT 
                    uniqExact(user_id) as DAU,
                    sum(action = 'like') as Likes,
                    sum(action = 'view') as Views,
                    round(Likes / Views, 3) as CTR
                FROM {db}.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY toDate(time) 
                """
query_last_week = """
                SELECT 
                  toString(day) as day,
                  DAU,
                  Likes,
                  Views,
                  CTR
                FROM 
                    (
                    SELECT 
                       toDate(time) as day,
                       uniqExact(user_id) as DAU,
                       sum(action = 'like') as Likes,
                       sum(action = 'view') as Views,
                       round(Likes / Views, 3) as CTR,
                       toString((SELECT min(toDate(time)) as start_period FROM simulator_20221120.feed_actions
                       WHERE dateDiff('day', toDate(time), today()) BETWEEN 1 AND 7)) as start_period,
                       toString((SELECT max(toDate(time)) as start_period FROM simulator_20221120.feed_actions
                       WHERE dateDiff('day', toDate(time), today()) BETWEEN 1 AND 7)) as end_period
                    FROM simulator_20221120.feed_actions
                    WHERE dateDiff('day', toDate(time), today()) BETWEEN 1 AND 7
                    GROUP BY day
                    ORDER BY day
                    ) 
                """
connection_simulator_20221120 = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'database': 'simulator_20221120',
        'user': 'student',
        'password': 'dpo_python_2020'
        }

yesterday_metrics = ph.read_clickhouse(query=query_yesterday, connection=connection_simulator_20221120)
last_week_metrics = ph.read_clickhouse(query=query_last_week, connection=connection_simulator_20221120)
def to_text(df):
    strs = []
    for c in df.columns:
        strs.append(f"{c}: {df['{}'.format(c)][0]}")
    text = '\n'.join(strs)
    return text
def get_plot(last_week_metrics):
    fig = plt.figure(figsize=(11, 9))

    ax_1 = fig.add_subplot(2, 1, 1)
    ax_1.plot(last_week_metrics.day, last_week_metrics.DAU)
    ax_1.set_title("DAU", fontweight="bold")

    ax_2 = fig.add_subplot(2, 2, 3)
    ax_2.plot(last_week_metrics.day, last_week_metrics.Likes, label='Лайки')
    ax_2.plot(last_week_metrics.day, last_week_metrics.Views, label='Просмотры')
    ax_2.set_title('Лайки и просмотры', fontweight="bold")
    ax_2.legend()
    plt.xticks(rotation=45)

    ax_3 = fig.add_subplot(2, 2, 4)
    ax_3.plot(last_week_metrics.day, last_week_metrics.CTR)
    ax_3.set_title('CTR', fontweight="bold")
    plt.xticks(rotation=45)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'previos_week_report.png'
    plt.close()

    return plot_object
def get_text(yesterday_metrics):

    header = f'Ежедневный отчет о ключевых метриках ленты\nдата:\
     {datetime.date.today().day}/{datetime.date.today().month}/{datetime.date.today().year}\n'
    yesterday_report = f'Ключевые метрики за вчерашний день:\n{to_text(yesterday_metrics)}\n'
    week_report = 'Ключевые метрики в динамике за предыдущие 7 дней на прикрепленной картинке ниже.'
    report = header +'\n'+ yesterday_report +'\n'+ week_report

    return report

#Настройка AirFlow
default_args = {
    'owner': 'da-pasechnik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2022, 3, 10),
}
schedule_interval = '55 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_daily_report_feed():
    @task
    def get_report():
        text = get_text(yesterday_metrics)
        photo = get_plot(last_week_metrics)
        rep = {'text': text, 'photo': photo}
        return rep

    @task
    def send_report(report, chat=None):
        chat_id = chat or 506862065
        bot.sendMessage(chat_id=chat_id, text=report['text'])
        bot.sendPhoto(chat_id=chat_id, photo=report['photo'])

    rep = get_report()
    send_report(rep)

dag_daily_report_feed = dag_daily_report_feed()

