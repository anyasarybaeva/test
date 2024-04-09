import requests
import psycopg2
import schedule
import time

# Функция для получения данных через API
def fetch_data_from_api():
    api_url = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
    params = {
        "size":10
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data from API")
        return None

# Функция для загрузки данных в Greenplum
def load_data_to_greenplum(data):
    # Подключение к базе данных Greenplum
    conn = psycopg2.connect(
        dbname="YOUR_DATABASE_NAME",
        user="YOUR_DATABASE_USER",
        password="YOUR_DATABASE_PASSWORD",
        host="YOUR_DATABASE_HOST",
        port="YOUR_DATABASE_PORT"
    )
    cursor = conn.cursor()

    # Пример загрузки данных в таблицу (замените на свои данные)
    for record in data:
        cursor.execute("INSERT INTO your_table (column1, column2) VALUES (%s, %s)", (record['column1'], record['column2']))

    conn.commit()
    cursor.close()
    conn.close()

# Функция для выполнения ELT процесса
def run_elt_process():
    data = fetch_data_from_api()
    if data:
        load_data_to_greenplum(data)
        print("ELT process completed successfully")
    else:
        print("ELT process failed")

# Планирование выполнения ELT процесса каждые 12 часов
schedule.every(12).hours.do(run_elt_process)

# Запуск планировщика
while True:
    schedule.run_pending()
    time.sleep(1)
