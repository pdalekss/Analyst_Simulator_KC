from airflow.decorators import dag, task
from datetime import timedelta
import datetime

#Настройка AirFlow
default_args = {
    'owner': 'da-pasechnik',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2022, 3, 10),
}
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_daily_report_app():

    import pandahouse as ph
    import telegram
    import seaborn as sns
    import matplotlib.pyplot as plt
    import io
    sns.set(font_scale=1.2)

    # Параметры бота
    token = '5937135186:AAEP_nEJ3HJ8-k7mQYQ5HSTTP67ZwVHBMUE'
    bot = telegram.Bot(token=token)

    # Параметры подключения CH
    using_servise_q = """
    SELECT 
      toString(day) as day,
      using_service,
      uniqExact(user_id) as cnt_users
    FROM 
        (
        SELECT 
          user_id,
          multiIf((use_feed = 1 AND use_messege = 0), 'лента', 
                  (use_feed = 0 AND use_messege = 1), 'мессенджер',
                  (use_feed = 1 AND use_messege = 1), 'лента+мессенджер', 'error') as using_service,
          if(toString(day_m) = '1970-01-01', day_f, day_m) as day
        FROM (
              SELECT 
                l.user_id as user_id,
                l.use_feed as use_feed,
                r.use_messege as use_messege,
                l.day as day_f,
                r.day as day_m
              FROM (
                    SELECT 
                      user_id,
                      1 as use_feed,
                      toDate(toStartOfDay(time)) as day
                    FROM simulator_20221120.feed_actions
                    GROUP BY user_id, day
                    ) as l 
              FULL OUTER JOIN (
                    SELECT 
                      user_id,
                      1 as use_messege,
                      toDate(toStartOfDay(time)) as day
                    FROM simulator_20221120.message_actions 
                    GROUP BY user_id, day
                    ) as r 
              ON l.user_id  = r.user_id AND l.day = r.day
            )
        WHERE dateDiff('day', toDate(day), today()) BETWEEN 1 AND 7
        )
    GROUP BY day, using_service
    ORDER BY day, using_service
    """
    feed_actions_q = """
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
    mess_sent_q = """
    SELECT 
      toDate(time) as day,
      COUNT(user_id) mess_sent 
    FROM simulator_20221120.message_actions 
    WHERE dateDiff('day', toDate(time), today()) BETWEEN 1 AND 7
    GROUP BY day
    ORDER BY day
    """
    new_posts_yesterday_q = """
    SELECT 
      toDate(public_time) AS day,
      uniqExact(post_id) AS new_posts
    FROM (
      SELECT 
        post_id,
        toStartOfDay(MIN(time)) AS public_time
      FROM simulator_20221120.feed_actions 
      GROUP BY post_id
      ORDER BY public_time
      )
    WHERE toDate(public_time) = yesterday()
    GROUP BY day
    ORDER BY day
    """
    new_users_yesterday_q = """
    SELECT
      user_id,
      toDate(toStartOfDay(MIN(time))) AS first_time
    FROM
        (
        SELECT 
          user_id,
          time 
        FROM simulator_20221120.feed_actions 

        union all

        SELECT 
          user_id,
          time 
        FROM simulator_20221120.message_actions 

        union distinct

        SELECT 
          reciever_id as user_id,
          time 
        FROM simulator_20221120.message_actions
        )
    GROUP BY user_id
    HAVING first_time = yesterday()
    """
    gone_users_yesterday_q = """
    SELECT
      user_id,
      toDate(toStartOfDay(MAX(time))) AS last_time
    FROM
        (
        SELECT 
          user_id,
          time 
        FROM simulator_20221120.feed_actions 

        union all

        SELECT 
          user_id,
          time 
        FROM simulator_20221120.message_actions 

        union distinct

        SELECT 
          reciever_id as user_id,
          time 
        FROM simulator_20221120.message_actions
        )
    GROUP BY user_id
    HAVING dateDiff('day', last_time, yesterday()) > 7
    """
    gone_users_2d_q = """
    SELECT
      user_id,
      toDate(toStartOfDay(MAX(time))) AS last_time
    FROM
        (
        SELECT 
          user_id,
          time 
        FROM simulator_20221120.feed_actions 

        union all

        SELECT 
          user_id,
          time 
        FROM simulator_20221120.message_actions 

        union distinct

        SELECT 
          reciever_id as user_id,
          time 
        FROM simulator_20221120.message_actions
        )
    GROUP BY user_id
    HAVING dateDiff('day', last_time, yesterday() - 1) > 7
    order by last_time
    """

    connection_simulator_20221120 = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'database': 'simulator_20221120',
        'user': 'student',
        'password': 'dpo_python_2020'
    }

    # Вытаскиваем и обрабатываем данные
    # EXTRACT
    @task
    def get_num_mess_yesterday(mess_sent):
        mess_sent_yesterday = mess_sent.copy(deep=True).tail(1).reset_index(drop=True)
        return mess_sent_yesterday.mess_sent[0]
    @task
    def get_gone_users_yesterday(query_y=gone_users_yesterday_q,
                                 query_2d=gone_users_2d_q,
                                 connection=connection_simulator_20221120):
        gone_users_yesterday = ph.read_clickhouse(query=query_y, connection=connection)
        gone_users_2d = ph.read_clickhouse(query=query_2d, connection=connection)
        delta = gone_users_yesterday.user_id.nunique() - gone_users_2d.user_id.nunique()
        return delta
    @task
    def get_num_new_users_yesterday(query=new_users_yesterday_q, connection=connection_simulator_20221120):
        new_users_yesterday = ph.read_clickhouse(query=query, connection=connection)
        return new_users_yesterday.user_id.nunique()
    @task
    def get_num_new_posts_yesterday(query=new_posts_yesterday_q, connection=connection_simulator_20221120):
        new_posts_yesterday = ph.read_clickhouse(query=query, connection=connection)
        return new_posts_yesterday.new_posts[0]
    @task
    def get_mess_sent(query=mess_sent_q, connection=connection_simulator_20221120):
        mess_sent = ph.read_clickhouse(query=query, connection=connection)
        return mess_sent
    @task
    def get_feed_actions(query=feed_actions_q, connection=connection_simulator_20221120):
        feed_actions = ph.read_clickhouse(query=query, connection=connection)
        return feed_actions
    @task
    def get_using_servise(query=using_servise_q, connection=connection_simulator_20221120):
        using_servise = ph.read_clickhouse(query=query, connection=connection)
        total_by_day = using_servise \
            .groupby('day', as_index=False) \
            .agg({'cnt_users': 'sum'}) \
            .rename(columns={'cnt_users': 'total'})
        using_servise_per = using_servise.merge(total_by_day, on='day')
        using_servise_per['per'] = round(using_servise_per.cnt_users / using_servise_per.total, 3)
        return using_servise_per
    # Формирование отчета
    # TRANSFORM
    @task
    def get_report(num_mess_yesterday, gone_users_yesterday, num_new_users_yesterday,
                   num_new_posts_yesterday, feed_actions, mess_sent, using_servise):
        # Формирование текста отчета
        def get_text(num_mess_yesterday, gone_users_yesterday, num_new_users_yesterday, num_new_posts_yesterday):
            header = f'Ежедневный отчет о ключевых показателях приложения\nдата: {datetime.date.today().day}/{datetime.date.today().month}/{datetime.date.today().year}\n'
            n_n_u = f'Число новых пользователей: {num_new_users_yesterday}\n'
            n_g_u = f'Число пользователей, не заходивших в приложение более 7 дней: {gone_users_yesterday}\n'
            n_n_p = f'Число новых постов: {num_new_posts_yesterday}\n'
            n_m = f'Число сообщений: {num_mess_yesterday}\n'
            yesterday_report = f'Показатели активности за вчерашний день:\n' + n_n_u + n_g_u + n_n_p + n_m
            week_report = 'Ключевые метрики в динамике за предыдущие 7 дней на прикрепленной картинке ниже.'
            report = header + '\n' + yesterday_report + '\n' + week_report

            return report
        # Формирование дашборда
        def get_plot(feed_actions, mess_sent, using_servise):
            fig = plt.figure(figsize=(15, 12))

            ax_1 = fig.add_subplot(2, 2, 1)
            sns.lineplot(data=using_servise, x='day', y='cnt_users', hue='using_service')
            ax_1.set_title('Использование сервисов', fontweight="bold")
            ax_1.set_xlabel('')
            ax_1.set_ylabel('Количество пользователей')
            plt.xticks(rotation=45)

            ax_2 = fig.add_subplot(2, 2, 2)
            using_servise \
                .pivot(index='day', columns='using_service', values='per') \
                .plot(kind='bar', stacked=True, rot=45, ax=ax_2)
            ax_2.set_title('Использование сервисов, %', fontweight="bold")
            ax_2.set_xlabel('')
            ax_2.set_ylabel('')

            ax_3 = fig.add_subplot(2, 3, 4)
            ax_3.plot(feed_actions.day, feed_actions.Likes, label='Лайки')
            ax_3.plot(feed_actions.day, feed_actions.Views, label='Просмотры')
            ax_3.set_title('Лайки и просмотры', fontweight="bold")
            plt.xticks(rotation=45)

            ax_4 = fig.add_subplot(2, 3, 5)
            ax_4.plot(feed_actions.day, feed_actions.CTR)
            ax_4.set_title('CTR', fontweight="bold")
            plt.xticks(rotation=45)

            ax_5 = fig.add_subplot(2, 3, 6)
            ax_5.plot(mess_sent.day, mess_sent.mess_sent)
            ax_5.set_title('Количество отправленных сообщений', fontweight="bold")
            plt.xticks(rotation=45)

            plt.subplots_adjust(wspace=0.3, hspace=0.5)
            fig.suptitle('Ключевые метрики за предыдущие 7 дней', fontweight="bold", fontsize=16)

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'previos_week_app_report.png'
            plt.close()

            return plot_object

        text = get_text(num_mess_yesterday, gone_users_yesterday, num_new_users_yesterday, num_new_posts_yesterday)
        photo = get_plot(feed_actions, mess_sent, using_servise)
        rep = {'text': text, 'photo': photo}
        return rep
    @task
    def send_report(report, chat=None):
        chat_id = chat or 506862065
        bot.sendMessage(chat_id=chat_id, text=report['text'])
        bot.sendPhoto(chat_id=chat_id, photo=report['photo'])

    #EXTRACT
    num_mess_yesterday = get_num_mess_yesterday(get_mess_sent())
    gone_users_yesterday = get_gone_users_yesterday()
    num_new_users_yesterday = get_num_new_users_yesterday()
    num_new_posts_yesterday = get_num_new_posts_yesterday()
    feed_actions = get_feed_actions()
    using_servise = get_using_servise()
    mess_sent = get_mess_sent()

    #TRANSFORM
    rep = get_report(num_mess_yesterday, gone_users_yesterday, num_new_users_yesterday,
                   num_new_posts_yesterday, feed_actions, mess_sent, using_servise)
    #LOAD
    send_report(report=rep, chat=-817095409)

dag_daily_report_feed = dag_daily_report_app()

