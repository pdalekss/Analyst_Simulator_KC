import telegram
import report
from datetime import datetime, timedelta
from airflow.decorators import dag, task

#Параметры бота
token = '5937135186:AAEP_nEJ3HJ8-k7mQYQ5HSTTP67ZwVHBMUE'
bot = telegram.Bot(token=token)

#Настройка AirFlow
default_args = {
    'owner': 'da-pasechnik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}
schedule_interval = '55 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_daily_report_feed():

    @task
    def get_report():
        text = report.report
        photo = report.get_plot()
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

