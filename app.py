from flask import Flask, render_template, request, redirect, url_for
import pika

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/collect_data', methods=['POST'])
def collect_data():
    age = int(request.form['age'])
    answer_1 = int(request.form['answer_1'])
    answer_2 = int(request.form['answer_2'])
    answer_3 = int(request.form['answer_3'])
    answer_4 = int(request.form['answer_4'])
    answer_5 = int(request.form['answer_5'])
    answer_6 = int(request.form['answer_6'])
    answer_7 = int(request.form['answer_7'])
    answer_8 = int(request.form['answer_8'])
    answer_9 = int(request.form['answer_9'])
    answer_10 = int(request.form['answer_10'])

    send_to_rabbitmq(age, answer_1, answer_2, answer_3, answer_4,
                     answer_5, answer_6, answer_7, answer_8, answer_9,
                     answer_10)

    return redirect(url_for('index'))


@app.route('/statistics')
def statistics():
    return render_template('statistics.html')


def send_to_rabbitmq(age, answer_1, answer_2, answer_3, answer_4, answer_5,
                     answer_6, answer_7, answer_8, answer_9, answer_10):
    '''
    Функция, устанавливающая соединение с RabbitMQ и
    отправляющая данные в очередь сообщений.
    '''
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='data_queue')
    payload = (f'{age}, {answer_1}, {answer_2}, {answer_3}, {answer_4}, {answer_5}, {answer_6}, {answer_7}, {answer_8}, {answer_9}, {answer_10}')
    channel.basic_publish(exchange='', routing_key='data_queue', body=payload)
    connection.close()


if __name__ == '__main__':
    app.run(debug=True)