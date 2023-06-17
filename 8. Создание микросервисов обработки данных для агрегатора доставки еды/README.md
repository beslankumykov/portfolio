# Создание микросервисов обработки данных для агрегатора доставки еды

## Архитектура решения
![Image](https://github.com/beslankumykov/portfolio/assets/87646293/b4b73ac2-e9f6-4391-a3be-9b7d1cfc7a75)

## Процесс релиза сервиса в Kubernetes
1. Создать Dockerfile на основе шаблона
2. Запушить образ в Container Registry
3. Развернуть образ в Kubernetes
4. Проверить с помощью kubectl, что сервис поднялся
5. Убедиться, что таблицы в Postgres заполняются данными

## Модель детального слоя
![Image (2)](https://github.com/beslankumykov/portfolio/assets/87646293/bd479fae-29da-4f6a-b8b9-a093d5d0ba61)
