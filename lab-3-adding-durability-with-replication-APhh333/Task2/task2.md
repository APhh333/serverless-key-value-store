# 2. Leader  node receives writes and event-sources (fan-out) them with some log structure (Kafka, RabbitMQ, physical file on s3, etc). * think about delivery order and versions.

## Вносимо зміни до файлів shard\main.py, coordinator\main.py
## Запускаємо контейнери

![2.2.png](./2.2.png)

## Перевіряємо регістрацію реплік на координаторі. 

![2.1.png](./2.1.png)

## Перевіряємо роботу координатора, лідерів та фоловерів. 
## Створюємо користувача. 
![2.3.png](./2.3.png)

## Користувач з'явився на лідері.
![2.4.png](./2.4.png)

## Користувач з'явився на фоловері.
![2.5.png](./2.5.png)

![2.6.png](./2.6.png)

## Update користувача
![2.7.png](./2.7.png)

## Оновлений користувач з'явився на фоловері.
![2.8.png](./2.8.png)

## Оновлений користувач з'явився на фоловері.
![2.9.png](./2.9.png)

## Видаляємо користувача та перевіряємо.
![2.10.png](./2.10.png)

