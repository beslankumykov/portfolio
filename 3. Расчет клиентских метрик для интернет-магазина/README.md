# Расчет клиентских метрик для интернет-магазина

## Спецификация API
**POST /generate_report**
Метод /generate_report инициализирует формирование отчёта. Пример вызова:
```bash
curl --location --request POST 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report' \
--header 'X-Nickname: {{ nickname }}' \
--header 'X-Cohort: {{ cohort_number }}' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}' \
--data-raw '' 
```
Метод возвращает task_id — ID задачи, в результате выполнения которой должен сформироваться отчёт.

**GET /get_report**
Метод get_report используется для получения отчёта после того, как он будет сформирован на сервере. Пример вызова:
```bash
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id={{ task_id }}' \
--header 'X-Nickname: {{ your_nickname }}' \
--header 'X-Cohort: {{ your_cohort_number }}' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}' 
```
Пока отчёт будет формироваться, будет возвращаться статус RUNNING. </br>
Если отчёт сформирован, то метод вернёт статус SUCCESS и report_id. </br>
Сформированный отчёт содержит четыре файла: custom_research.csv, user_order_log.csv, user_activity_log.csv, price_log.csv. </br>
Файлы отчетов можно получить по URL из параметра s3_path или сформировать URL самостоятельно по следующему шаблону:
```
https://storage.yandexcloud.net/s3-sprint3/cohort_{{ cohort_number }}/{{ nickname }}/project/{{ report_id }}/{{ file_name }}
```

**GET /get_increment**
Метод get_increment используется для получения данных за те даты, которые не вошли в основной отчёт. Дата в формате 2020-01-22T00:00:00. Пример вызова:
```bash
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_increment?report_id={{ report_id }}&date={{ date }}' \
--header 'X-Nickname: {{ your_nickname }}' \
--header 'X-Cohort: {{ your_cohort_number }}' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}' 
```
Если инкремент сформирован, то метод вернёт статус SUCCESS и increment_id. 
Если инкремент не сформируется, то вернётся NOT FOUND с описанием причины.
Сформированный инкремент содержит четыре файла: custom_research_inc.csv, user_order_log_inc.csv, user_activity_log_inc.csv, price_log_inc.csv.
Файлы отчетов можно получить по URL из параметра s3_path или сформировать URL самостоятельно по следующему шаблону:
```
https://storage.yandexcloud.net/s3-sprint3/cohort_{{ cohort_number }}/{{ nickname }}/project/{{ increment_id }}/{{ file_name }}
```

## Описание задачи
1. Учесть в витрине mart.f_sales статусы shipped и refunded, использовать только shipped.
2. Провести миграцию схемы и данных в таблице mart.f_sales.
3. Обновить пайплайн с учётом статусов и backward compatibility.
4. Создать витрину mart.f_customer_retention со следующими атрибутами:
- new_customers_count — кол-во новых клиентов (тех, которые сделали только один заказ за рассматриваемый промежуток времени).
- returning_customers_count — кол-во вернувшихся клиентов (тех, которые сделали только несколько заказов за рассматриваемый промежуток времени).
- refunded_customer_count — кол-во клиентов, оформивших возврат за рассматриваемый промежуток времени.
- period_name — weekly.
- period_id — идентификатор периода (номер недели или номер месяца).
- item_id — идентификатор категории товара.
- new_customers_revenue — доход с новых клиентов.
- returning_customers_revenue — доход с вернувшихся клиентов.
- customers_refunded — количество возвратов клиентов. 
5. Перезапустить пайплайн и убедиться, что после перезапуска не появилось дубликатов в витринах mart.f_sales и mart.f_customer_retention.
