# producer_pg_to_kafka.py
import psycopg2
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute("SELECT username, event_type, extract(epoch FROM event_time), sent_to_kafka FROM user_logins")
rows = cursor.fetchall()

for row in rows:
    if row[3] == False: #Проверка на дубликат, можно убрать конструкцию if , но добавить в запрос WHERE sent_to_kafka is false
        data = {
            "user": row[0],
            "event": row[1],
            "timestamp": float(row[2])  # преобразуем Decimal → float
        }
        producer.send("user_events", value=data)
        print("Sent:", data)
        #Обновление флага после записи в в кафку
        update_cursor = conn.cursor()
        update_cursor.execute("UPDATE user_logins SET sent_to_kafka = True WHERE username = %s AND event_type = %s AND event_time = to_timestamp(%s)", (row[0], row[1], row[2]))
        conn.commit()
        update_cursor.close()

        time.sleep(0.5)

cursor.close()
conn.close()