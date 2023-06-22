# Настройка потоковой обработки данных для агрегатора доставки еды

## Описание проекта

### Схема работы
![Image (1)](https://github.com/beslankumykov/portfolio/assets/87646293/0569469c-09b4-42aa-ae95-6ad22f7dd8c0)

Система работает так:
1. Ресторан отправляет через мобильное приложение акцию с ограниченным предложением. Например, такое: «Дарим на новое блюдо скидку 70% до 14:00!».
2. Сервис проверяет, у кого из пользователей ресторан находится в избранном списке.
3. Сервис формирует заготовки для push-уведомлений этим пользователям о временных акциях. Уведомления будут отправляться только пока действует акция.

На техническом уровне сервис:
1. Читает данные из Kafka с помощью Spark Structured Streaming и Python в режиме реального времени
2. Получает список подписчиков из базы данных Postgres
3. Джойнит данные из Kafka с данными из БД по полю restaurant_id
4. Сохраняет в памяти полученные данные, чтобы не собирать их заново после отправки в Postgres или Kafka
5. Отправляет выходное сообщение в Kafka с информацией об акции, пользователе со списком избранного и ресторане.
6. Вставляет записи в Postgres, чтобы получить фидбэк от пользователя.

### Отправка тестового сообщения в Kafka с помощью kcat:
```bash
kafkacat -b host:port
-X security.protocol=SASL_SSL
-X sasl.mechanisms=SCRAM-SHA-512
-X sasl.username="username"
-X sasl.password="password"
-X ssl.ca.location=/home/ca.crt
-t topic_in
-K:
-P 
```
Пример сообщения:
```
first_message:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
```
, где:
- "restaurant_id": "123e4567-e89b-12d3-a456-426614174000" — UUID ресторана
- "adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003" — UUID рекламной кампании
- "adv_campaign_content": "first campaign" — текст кампании
- "adv_campaign_owner": "Ivanov Ivan Ivanovich" — сотрудник ресторана, который является владельцем кампании
- "adv_campaign_owner_contact": "iiivanov@restaurant.ru" — его контакт
- "adv_campaign_datetime_start": 1659203516 — время начала рекламной кампании в формате timestamp
- "adv_campaign_datetime_end": 2659207116 — время её окончания в формате timestamp
- "datetime_created": 1659131516 — время создания кампании в формате timestamp

Для выходного сообщения добавится два новых поля:
- "client_id":"023e4567-e89b-12d3-a456-426614174000" - UUID подписчика ресторана, который достаётся из таблицы Postgres
- "trigger_datetime_created":1659304828 - время создания выходного сообщения

## Стек технологий

`Python` `PostgreSQL` `SQL` `Kafka` `Spark Streaming`

## Статус проекта

Завершен.
