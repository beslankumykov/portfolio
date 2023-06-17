# Настройка потоковой обработки данных для агрегатора доставки еды
Написать сервис, который будет:
1. Читать данные из Kafka с помощью Spark Structured Streaming и Python в режиме реального времени.
2. Получать список подписчиков из базы данных Postgres.
3. Джойнить данные из Kafka с данными из БД.
4. Сохранять в памяти полученные данные, чтобы не собирать их заново после отправки в Postgres или Kafka.
5. Отправлять выходное сообщение в Kafka с информацией об акции, пользователе со списком избранного и ресторане.
6. Вставлять записи в Postgres, чтобы получить фидбэк от пользователя.

Запуск продюсера для теста:
```bash
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091
-X security.protocol=SASL_SSL
-X sasl.mechanisms=SCRAM-SHA-512
-X sasl.username="de-student"
-X sasl.password="ltcneltyn"
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt
-t student.topic.cohort6.kubes00_in
-K:
-P 
```
Пример сообщения:
```
key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
```
