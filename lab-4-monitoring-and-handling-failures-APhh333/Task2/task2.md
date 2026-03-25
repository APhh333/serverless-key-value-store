# 1. Conflict Resolution (5)

# a. Implement versioning for records (vector clocks or timestamps). (2 points) 

## Для виконання був обраний datadog і проведене тестування навантаження  за допомогою YCSB
![2.1.png](./2.1.png)
## Реалізація в коді шарду

# b. Implement Last-Write-Wins (LWW) conflict resolution strategy. (2 points)

![2.2.png](./2.2.png)
## Логіка вирішення конфлікту

## Тепер проведемо експеримент: 

![2.3.png](./2.3.png)
## Створюмо еталонний запис через Координатор

![2.4.png](./2.4.png)
## Спробуємо зламати систему старим запитом
![2.5.png](./2.5.png)
## Перевірка змін

![2.6.png](./2.6.png)
## Тепер оновлюємо систему майбутнім запитом

![2.7.png](./2.7.png)
![2.8.png](./2.8.png)
## Перевірка змін


# c. (Optional) Add alternative strategy: custom merge function or client-side conflict resolution. (1 point)
![2.9.png](./2.9.png)
## Реалізація стратегії злиття для списків

![2.10.png](./2.10.png)

## Для демонстрації гнучкості системи було реалізовано спеціальну логіку злиття (Merge) для списків. 
## Замість повної заміни даних (як у LWW), нові елементи додаються до існуючого списку без дублікатів.



