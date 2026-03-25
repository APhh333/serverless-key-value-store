# 4. (Optional) For reads implement quorum with R+W>N rule with W=1 (for single-leader, >1 for leaderless), where N - total replicas for a key (replication factor). W - number of replicas that must durably ack a write before it “commits”. R - number of replicas a read queries; the coordinator returns the newest version among them.

## Запускаємо контейнери (docker-compose up --build)

![4.1.png](./4.1.png)

## Створення нового ключа. 

![4.2.png](./4.2.png)

## Вимикаємо репліку Шард 2 фоловер 1.
![4.3.png](./4.3.png)

## Оновив ключ поки репліка вимкнена (фоловер 1).
![4.4.png](./4.4.png)

# Тест! Читання Quorum.
## Запит на читання q1 до координатора. Видно тільки 2 шарди.
![4.5.png](./4.5.png)

## Координатор отримав 2 успішні відповіді (обидві з version_2) і 1 помилку. Він порівняв ті, що отримав, побачив, що найновіша — це version_2.
![4.6.png](./4.6.png)

## Система повертає найновіші дані, навіть якщо одна репліка (або навіть дві!) відстане чи вийде з ладу.
