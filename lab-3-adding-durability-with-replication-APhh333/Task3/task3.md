# 3. Read replicas (followers) consume log. 
# a. Note that they should be used not only for durability, but also for load distribution - read load should be evenly balanced. So controller should resolve reads to all 3+ active shards.

## Балансування читання
## Запускаємо контейнери

![3.1a.png](./3.1a.png)

## Приклади балансування. 

![3.2a.png](./3.2a.png)

# b. Followers should track offset and upon restart resync only relevant log range

## Відстеження "offset" та "resync" 
## Синхронізовані Шард 2 лідер 1. 
![3.3b.png](./3.3b.png)

## Синхронізовані Шард 2 фоловер 2.
![3.4b.png](./3.4b.png)

## Синхронізовані Шард 2 фоловер 1.
![3.5b.png](./3.5b.png)

## Зупиняємо шард 2 фоловер 1.
![3.6b.png](./3.6b.png)

## Ми внесли зміни до шарду 2 з вимкненим фоловером 1.
![3.7b.png](./3.7b.png)

## Демонстрація синхронізації фоловера 1 з шардом після його вмикання.
![3.8b.png](./3.8b.png)
