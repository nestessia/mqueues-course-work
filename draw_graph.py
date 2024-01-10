import plotly.express as px
import pandas as pd
import pika
import seaborn as sns
import matplotlib.pyplot as plt


def draw_graph_callback(ch, method, properties, body):
    '''Функция, записывающая данные в csv из данных очереди сообщений.'''
    payload = body.decode('utf-8')
    (age, answer_1, answer_2, answer_3, answer_4, answer_5, answer_6, answer_7, answer_8, answer_9, answer_10) = map(str, payload.split(','))
    stress_grade = (int(answer_1) + int(answer_2) + int(answer_3)
                    + int(answer_4) + int(answer_5) + int(answer_6) +
                    int(answer_7) + int(answer_8) + int(answer_9) +
                    int(answer_10))
    print(f"Полученное сообщение - возраст: {age}, ответы: {answer_1}, {answer_2}, {answer_3}, {answer_4}, {answer_5}, {answer_6}, {answer_7}, {answer_8}, {answer_9}, {answer_10}. Уровень стресса - {stress_grade}\n")
    data = [{'age': age, 'stress_level': stress_grade}]
    df = pd.DataFrame(data)
    df.to_csv('data_stress_level.csv',
              mode='a',
              index=False,
              header=not df_exists('data_stress_level.csv'))
    data2 = [{'age': age,
              'q1': answer_1,
              'q2': answer_2,
              'q3': answer_3,
              'q4': answer_4,
              'q5': answer_5,
              'q6': answer_6,
              'q7': answer_7,
              'q8': answer_8,
              'q9': answer_9,
              'q10': answer_10}]
    df2 = pd.DataFrame(data2)
    df2.to_csv('data_answers.csv', mode='a', index=False, header=not df_exists('data_answers.csv'))
    draw_graph_stress_level()
    draw_graph_correlation()


def df_exists(filename):
    try:
        pd.read_csv(filename)
        return True
    except FileNotFoundError:
        return False


def draw_graph_stress_level():
    '''
    Функция, строющая стобчатую диаграмму среднего уровеня стресса
    среди опрошенных разных возрастов.
    '''
    df = pd.read_csv('data_stress_level.csv')
    average_stress_by_age = df.groupby('age')['stress_level'].agg(['sum', 'count']).reset_index()
    average_stress_by_age['average_stress'] = (
        average_stress_by_age['sum']
        / average_stress_by_age['count'])
    fig = px.bar(average_stress_by_age, x='age', y='average_stress',
                 title='Средний уровень стресса среди опрошенных разных возрастов.',
                 labels={'average_stress': 'Средний уровень стресса', 'age': 'Возраст'},
                 range_y=[10, 50])
    fig.write_html('templates/graph_average.html')


def draw_graph_correlation():
    '''
    Корреляционный анализ с построением тепловой карты корреляции.
    '''
    df = pd.read_csv('data_answers.csv')
    correlation_matrix = df.corr()
    plt.figure(figsize=(12, 10))
    heatmap = sns.heatmap(correlation_matrix,
                          annot=True,
                          cmap='coolwarm',
                          fmt='.2f', linewidths=.5)
    plt.title('Тепловая карта корреляции')
    heatmap.get_figure().savefig('templates/correlation_graph.png')


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='data_queue')
    channel.basic_consume(queue='data_queue',
                          on_message_callback=draw_graph_callback,
                          auto_ack=True)
    print('В ожидании данных. Для выхода нажмите Ctrl+C')
    channel.start_consuming()
