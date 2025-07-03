# Перенос событий пользователей из PostgreSQL в ClickHouse через Kafka

В таблице user_logins базы данных PostgreSQL хранятся события пользователей, такие как логин, регистрация, покупка и другие. Для передачи этих событий в другую систему (например, ClickHouse) можно использовать Kafka как промежуточное звено.

---

## Архитектура передачи данных

PostgreSQL (CDC) --> Python producer --> Kafka (topic: userevents) --> Python consumer --> ClickHouse (userlogins)


---

## Инструкция по запуску

1. **Запустить инфраструктуру с помощью Docker Compose**

bash
docker-compose up -d


2. **Запустить продюсера**

Продюсер забирает данные из PostgreSQL и кладёт их в Kafka с проверкой на дубликаты.

bash
python producer.py


3. **Запустить консюмера**

Консюмер забирает данные из Kafka и записывает их в ClickHouse.

bash
python consumer.py


4. **Проверить данные в ClickHouse**

Подключитесь к ClickHouse и убедитесь, что данные успешно приземлились в таблицу user_logins.

---



